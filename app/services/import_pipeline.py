"""Import pipeline orchestrating the complete data import workflow.

Pipeline Steps:
1. Fetch source file - Use connector to retrieve file from remote location
2. Cut columns - Extract required columns from source file
3. Split by GFCID - Split processed file by GFCID column
4. Load to Neo4j - Import split files into Neo4j graph database
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional
from uuid import uuid4

from .connectors import (
    ColumnMapResolver,
    ConnectorConfig,
    ConnectorFactory,
    DataMapResolver,
    SettingsLoader,
)
from .processors import DataProcessor, ProcessResult
from .neo4j_loader import Neo4jLoader, LoadResult, create_loader_from_settings

logger = logging.getLogger(__name__)


class WorkflowStatus(str, Enum):
    """Workflow lifecycle states."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    FETCHING = "fetching"
    CUTTING = "cutting"
    SPLITTING = "splitting"
    LOADING = "loading"
    DONE = "done"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ImportRequest:
    """Inbound request to run an import."""
    domain_type: str
    domain_name: str
    cob_date: date

    @property
    def cob_date_str(self) -> str:
        """Return COB date as string (YYYYMMDD format)."""
        if isinstance(self.cob_date, str):
            return self.cob_date.replace("-", "")
        return self.cob_date.strftime("%Y%m%d")


@dataclass
class WorkflowState:
    """In-memory workflow tracking with detailed step information."""
    workflow_id: str
    status: WorkflowStatus
    message: Optional[str] = None
    current_step: Optional[str] = None
    steps_completed: List[str] = field(default_factory=list)
    files_created: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)


class ImportPipeline:
    """Orchestrate the complete import pipeline."""

    def __init__(
        self,
        data_map_resolver: Optional[DataMapResolver] = None,
        column_map_resolver: Optional[ColumnMapResolver] = None,
        settings_loader: Optional[SettingsLoader] = None,
        status_store: Optional[MutableMapping[str, WorkflowState]] = None,
    ):
        """
        Initialize the import pipeline.

        Args:
            data_map_resolver: Resolver for data source configuration
            column_map_resolver: Resolver for column mapping configuration
            settings_loader: Loader for application settings
            status_store: Optional store for workflow states
        """
        self.data_map_resolver = data_map_resolver or DataMapResolver()
        self.column_map_resolver = column_map_resolver or ColumnMapResolver()
        self.settings_loader = settings_loader or SettingsLoader()
        self.status_store: MutableMapping[str, WorkflowState] = status_store or {}

    def run(
        self,
        request: ImportRequest,
        workflow_id: Optional[str] = None,
        skip_fetch: bool = False,
        skip_cut: bool = False,
        skip_split: bool = False,
        skip_load: bool = False,
    ) -> WorkflowState:
        """
        Execute the complete import pipeline.

        Args:
            request: Import request with domain and date info
            workflow_id: Optional workflow ID (generated if not provided)
            skip_fetch: Skip the file fetch step
            skip_cut: Skip the column cutting step
            skip_split: Skip the file splitting step
            skip_load: Skip the Neo4j loading step

        Returns:
            WorkflowState with final status
        """
        workflow_id = workflow_id or str(uuid4())
        state = WorkflowState(
            workflow_id=workflow_id,
            status=WorkflowStatus.PENDING,
            current_step="initializing"
        )
        self.status_store[workflow_id] = state

        try:
            # Validate request
            self._validate_request(request)

            # Load configurations
            settings = self.settings_loader.load()
            data_config = self.data_map_resolver.resolve(
                request.domain_type, request.domain_name
            )
            if not data_config:
                raise ValueError(
                    f"No data source configuration found for "
                    f"{request.domain_type}/{request.domain_name}"
                )

            column_config = self.column_map_resolver.resolve(request.domain_name)
            if not column_config:
                # Use defaults if no specific config
                column_config = self.column_map_resolver.get_defaults()
                logger.warning(f"Using default column config for {request.domain_name}")

            cob_date = request.cob_date_str

            # Get dropbox directory from settings
            dropbox_dir = settings.get("DROPBOX_DIR", "/mnt/nas")

            # Step 1: Fetch source file
            state.status = WorkflowStatus.FETCHING
            state.current_step = "fetch"
            self._update_state(state)

            if skip_fetch:
                logger.info("Skipping fetch step")
                source_path = self._get_source_path(data_config, cob_date)
            else:
                source_path = self._fetch_file(request, data_config, settings, state)
                if not source_path:
                    raise RuntimeError("Failed to fetch source file")

            state.steps_completed.append("fetch")
            state.files_created.append(str(source_path))

            # Step 2 & 3: Cut columns and split
            state.status = WorkflowStatus.CUTTING
            state.current_step = "process"
            self._update_state(state)

            processor = DataProcessor(column_config, dropbox_dir=dropbox_dir)
            process_results = processor.process_file(
                source_path,
                cob_date,
                skip_cut=skip_cut,
                skip_split=skip_split
            )

            # Check cut result
            cut_result = process_results.get("cut")
            if cut_result and not cut_result.success:
                raise RuntimeError(f"Column cutting failed: {cut_result.error}")

            if cut_result and cut_result.output_path:
                state.files_created.append(str(cut_result.output_path))
                state.metrics["rows_after_cut"] = cut_result.rows_processed

            state.steps_completed.append("cut")

            # Check split result
            state.status = WorkflowStatus.SPLITTING
            state.current_step = "split"
            self._update_state(state)

            split_result = process_results.get("split")
            if split_result and not split_result.success:
                raise RuntimeError(f"File splitting failed: {split_result.error}")

            split_files = []
            if split_result and split_result.output_paths:
                split_files = split_result.output_paths
                state.files_created.extend([str(p) for p in split_files])
                state.metrics["split_files_count"] = len(split_files)

            state.steps_completed.append("split")

            # Step 4: Load to Neo4j
            state.status = WorkflowStatus.LOADING
            state.current_step = "load"
            self._update_state(state)

            if skip_load:
                logger.info("Skipping Neo4j load step")
            else:
                load_result = self._load_to_neo4j(split_files, settings, state, dropbox_dir)
                if not load_result.success:
                    logger.warning(f"Neo4j load had failures: {load_result.error}")
                    state.metrics["load_failed_files"] = load_result.failed_files

                state.metrics["nodes_created"] = load_result.nodes_created
                state.metrics["relationships_created"] = load_result.relationships_created

            state.steps_completed.append("load")

            # Complete
            state.status = WorkflowStatus.COMPLETED
            state.current_step = None
            state.message = f"Successfully processed {request.domain_name} for {cob_date}"
            self._update_state(state)

            logger.info(f"Pipeline completed for workflow {workflow_id}")
            return state

        except Exception as exc:
            state.status = WorkflowStatus.FAILED
            state.message = str(exc)
            self._update_state(state)
            logger.exception(f"Pipeline failed for workflow {workflow_id}: {exc}")
            raise

    def _validate_request(self, request: ImportRequest) -> None:
        """Validate the import request."""
        missing = []
        if not request.domain_type:
            missing.append("domain_type")
        if not request.domain_name:
            missing.append("domain_name")
        if not request.cob_date:
            missing.append("cob_date")

        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

    def _get_source_path(self, data_config: Dict[str, Any], cob_date: str) -> Path:
        """Get the expected source file path."""
        template = data_config.get("source_file_path_template", "")
        path_str = template.format(cob_date=cob_date, cob=cob_date)
        return Path(path_str)

    def _fetch_file(
        self,
        request: ImportRequest,
        data_config: Dict[str, Any],
        settings: Dict[str, Any],
        state: WorkflowState
    ) -> Optional[Path]:
        """Fetch the source file using the appropriate connector."""
        try:
            connector_type = data_config.get("connector_type", "linux")
            connector_params = data_config.get("connector_params", {})

            config = ConnectorConfig(
                connector_type=connector_type,
                server_name=connector_params.get("server_name"),
                params=connector_params
            )

            connector = ConnectorFactory.create(connector_type, config, settings)

            # Get source and destination paths
            cob_date = request.cob_date_str
            source_template = data_config.get("source_file_path_template", "")
            source_path = source_template.format(cob_date=cob_date, cob=cob_date)

            # Destination is in the dropbox directory
            dropbox_dir = settings.get("DROPBOX_DIR", "/mnt/nas")
            dest_filename = Path(source_path).name
            dest_path = Path(dropbox_dir) / dest_filename

            logger.info(f"Fetching {source_path} to {dest_path}")
            result = connector.fetch(source_path, dest_path)

            if result.success:
                state.metrics["bytes_fetched"] = result.bytes_transferred
                return result.local_path
            else:
                logger.error(f"Fetch failed: {result.error}")
                return None

        except Exception as e:
            logger.error(f"Error in fetch step: {e}")
            return None

    def _load_to_neo4j(
        self,
        file_paths: List[Path],
        settings: Dict[str, Any],
        state: WorkflowState,
        dropbox_dir: str = "/mnt/nas"
    ) -> LoadResult:
        """Load files to Neo4j."""
        try:
            loader = create_loader_from_settings(settings)
            if not loader:
                return LoadResult(
                    success=False,
                    error="Failed to create Neo4j loader - check configuration"
                )

            try:
                # Pass dropbox_dir as base_path for Neo4j LOAD CSV
                base_path = dropbox_dir.rstrip("/") + "/"
                result = loader.load_files(file_paths, base_path=base_path)
                return result
            finally:
                loader.close()

        except Exception as e:
            logger.error(f"Neo4j load error: {e}")
            return LoadResult(success=False, error=str(e))

    def _update_state(self, state: WorkflowState) -> None:
        """Update the state in the store."""
        self.status_store[state.workflow_id] = state


def build_default_pipeline(
    status_store: Optional[MutableMapping[str, WorkflowState]] = None,
    logger: Optional[logging.Logger] = None,
) -> ImportPipeline:
    """
    Build an ImportPipeline with default configuration.

    Args:
        status_store: Optional store for workflow states
        logger: Optional logger instance

    Returns:
        Configured ImportPipeline instance
    """
    pipeline = ImportPipeline(
        data_map_resolver=DataMapResolver(),
        column_map_resolver=ColumnMapResolver(),
        settings_loader=SettingsLoader(),
        status_store=status_store,
    )

    if logger:
        logger.debug("Default import pipeline constructed")

    return pipeline


# Legacy exports for backward compatibility
ParameterValidator = None  # Removed - validation now in pipeline
Neo4jTransactionChecker = None  # Removed - handled by loader
DataSourceConfig = None  # Removed - using ConnectorConfig
FetcherRegistry = None  # Removed - using ConnectorFactory
LocalFileFetcher = None  # Removed - using LinuxConnector
DataTransformer = None  # Removed - using DataProcessor
DataLoader = None  # Removed - using Neo4jLoader
PassthroughTransformer = None  # Removed
NoOpLoader = None  # Removed
