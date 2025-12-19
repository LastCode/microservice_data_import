"""Import pipeline scaffolding covering validation, extract, transform, and load."""
from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Dict, MutableMapping, Protocol, Sequence
from uuid import uuid4

from app.storage.graph import get_driver


class WorkflowStatus(str, Enum):
    """Workflow lifecycle states."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ImportRequest:
    """Inbound request to run an import."""

    domain_type: str
    domain_name: str
    cob_date: date


@dataclass
class WorkflowState:
    """In-memory workflow tracking."""

    workflow_id: str
    status: WorkflowStatus
    message: str | None = None


class ParameterValidator:
    """Validate inbound parameters."""

    def validate(self, request: ImportRequest) -> None:
        missing = [
            name
            for name, value in {
                "domain_type": request.domain_type,
                "domain_name": request.domain_name,
                "cob_date": request.cob_date,
            }.items()
            if value in (None, "")
        ]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")


class Neo4jTransactionChecker:
    """Check whether transactions already exist in Neo4j for a given COB/domain."""

    def exists(self, request: ImportRequest) -> bool:
        driver = get_driver()
        if driver is None:
            return False
        # TODO: Implement Cypher query to detect transactions for the given (domain_type, domain_name, cob_date).
        raise NotImplementedError("Add Cypher query for transaction existence check.")


@dataclass
class DataSourceConfig:
    """Resolved data source configuration from data_map.csv."""

    source_type: str
    location: str
    options: Dict[str, Any]


class DataMapResolver:
    """Resolve domain -> data source config from conf/data_map.csv."""

    def __init__(self, data_map_path: Path = Path("conf/data_map.csv")) -> None:
        self.data_map_path = data_map_path

    def resolve(self, request: ImportRequest) -> DataSourceConfig:
        if not self.data_map_path.exists():
            raise FileNotFoundError(f"data map not found: {self.data_map_path}")

        with self.data_map_path.open("r", newline="") as handle:
            reader = csv.DictReader(handle)
            target_type = request.domain_type.strip().upper()
            target_name = request.domain_name.strip().lower()

            for row in reader:
                row_type = (row.get("domain_type") or "").strip().upper()
                row_name = (row.get("domain_name") or "").strip().lower()
                if row_type != target_type or row_name != target_name:
                    continue

                options_raw = row.get("data_source_parameters") or "{}"
                try:
                    options = json.loads(options_raw.replace('""', '"'))
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Invalid data_source_parameters JSON for {row_name}") from exc

                return DataSourceConfig(
                    source_type=(row.get("data_source_type") or "").strip(),
                    location=row.get("pyhysical_name") or "",
                    options=options,
                )

        raise ValueError(
            f"No data source config found for domain_type={request.domain_type}, domain_name={request.domain_name}"
        )


class DataFetcher(Protocol):
    """Fetcher contract for pulling data from different source types."""

    def fetch(self, config: DataSourceConfig, request: ImportRequest) -> Path:
        ...


class FetcherRegistry:
    """Registry to look up fetchers by source_type."""

    def __init__(self) -> None:
        self._fetchers: Dict[str, DataFetcher] = {}

    def register(self, source_type: str, fetcher: DataFetcher) -> None:
        self._fetchers[source_type.lower()] = fetcher

    def get(self, source_type: str) -> DataFetcher:
        key = source_type.lower()
        if key not in self._fetchers:
            raise KeyError(f"No fetcher registered for source_type={source_type}")
        return self._fetchers[key]


class LocalFileFetcher:
    """Fetcher for files reachable on the local filesystem (e.g., NAS mount)."""

    def fetch(self, config: DataSourceConfig, request: ImportRequest) -> Path:
        template = config.options.get("file_path") or config.location
        if not template:
            raise ValueError("file_path is required for local fetcher")

        file_path = Path(
            template.format(
                cob=request.cob_date_1.isoformat(),
                domain_type=request.domain_type,
                domain_name=request.domain_name,
            )
        )
        if not file_path.exists():
            raise FileNotFoundError(f"file not found: {file_path}")
        return file_path


class DataTransformer:
    """Data cleaning and splitting operations."""

    def clean(self, raw_path: Path) -> Path:
        # TODO: implement cleaning, type conversion, missing value handling.
        raise NotImplementedError

    def cut_columns(self, cleaned_path: Path) -> Path:
        # TODO: implement column slicing to a predefined schema.
        raise NotImplementedError

    def split_by_gfcid(self, cut_path: Path) -> Sequence[Path]:
        # TODO: split into batch files by GFCID.
        raise NotImplementedError


class DataLoader:
    """Load transformed batches into Neo4j."""

    def ensure_indexes_and_constraints(self, request: ImportRequest) -> None:
        # TODO: create indexes/constraints prior to ingest.
        raise NotImplementedError

    def create_nodes(self, batches: Sequence[Path], request: ImportRequest) -> None:
        # TODO: load nodes (can be parallelized).
        raise NotImplementedError

    def create_relationships(self, batches: Sequence[Path], request: ImportRequest) -> None:
        # TODO: load relationships (can be parallelized).
        raise NotImplementedError


class ImportPipeline:
    """Orchestrate validate -> extract -> transform -> load steps."""

    def __init__(
        self,
        validator: ParameterValidator,
        transaction_checker: Neo4jTransactionChecker,
        data_map_resolver: DataMapResolver,
        fetcher_registry: FetcherRegistry,
        transformer: DataTransformer,
        loader: DataLoader,
        status_store: MutableMapping[str, WorkflowState] | None = None,
    ) -> None:
        self.validator = validator
        self.transaction_checker = transaction_checker
        self.data_map_resolver = data_map_resolver
        self.fetcher_registry = fetcher_registry
        self.transformer = transformer
        self.loader = loader
        self.driver = get_driver()
        self.status_store: MutableMapping[str, WorkflowState] = status_store or {}

    def run(self, request: ImportRequest, workflow_id: str | None = None) -> WorkflowState:
        """Execute the pipeline; update status_store throughout."""
        workflow_id = workflow_id or str(uuid4())
        state = WorkflowState(workflow_id=workflow_id, status=WorkflowStatus.PENDING)
        self.status_store[workflow_id] = state

        try:
            self.validator.validate(request)
            if self.transaction_checker.exists(request):
                state.status = WorkflowStatus.DONE
                state.message = "Data already exists for given domain/cob_date_1"
                return state

            state.status = WorkflowStatus.IN_PROGRESS
            config = self.data_map_resolver.resolve(request)
            fetcher = self.fetcher_registry.get(config.source_type)
            raw_data = fetcher.fetch(config, request)
            cleaned = self.transformer.clean(raw_data)
            cut = self.transformer.cut_columns(cleaned)
            batches = list(self.transformer.split_by_gfcid(cut))

            self.loader.ensure_indexes_and_constraints(request)
            self.loader.create_nodes(batches, request)
            self.loader.create_relationships(batches, request)

            state.status = WorkflowStatus.COMPLETED
            return state
        except Exception as exc:
            state.status = WorkflowStatus.FAILED
            state.message = str(exc)
            raise


class PassthroughTransformer(DataTransformer):
    """Transformer stub that leaves data untouched but follows the interface."""

    def clean(self, raw_path: Path) -> Path:
        return raw_path

    def cut_columns(self, cleaned_path: Path) -> Path:
        return cleaned_path

    def split_by_gfcid(self, cut_path: Path) -> Sequence[Path]:
        return [cut_path]


class NoOpLoader(DataLoader):
    """Loader stub that does nothing (placeholder for real Neo4j load)."""

    def ensure_indexes_and_constraints(self, request: ImportRequest) -> None:
        return None

    def create_nodes(self, batches: Sequence[Path], request: ImportRequest) -> None:
        return None

    def create_relationships(self, batches: Sequence[Path], request: ImportRequest) -> None:
        return None


def build_default_pipeline(
    status_store: MutableMapping[str, WorkflowState] | None = None,
    logger: logging.Logger | None = None,
) -> ImportPipeline:
    """Assemble an ImportPipeline with default stub components."""
    validator = ParameterValidator()
    transaction_checker = Neo4jTransactionChecker()
    data_map_resolver = DataMapResolver()
    fetcher_registry = FetcherRegistry()
    fetcher_registry.register("linux", LocalFileFetcher())
    transformer = PassthroughTransformer()
    loader = NoOpLoader()

    pipeline = ImportPipeline(
        validator=validator,
        transaction_checker=transaction_checker,
        data_map_resolver=data_map_resolver,
        fetcher_registry=fetcher_registry,
        transformer=transformer,
        loader=loader,
        status_store=status_store,
    )
    if logger:
        logger.debug("Default import pipeline constructed with stub components")
    return pipeline
