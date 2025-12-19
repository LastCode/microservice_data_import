"""Neo4j data loader for importing transaction data."""
from __future__ import annotations

import csv
import logging
import multiprocessing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from neo4j import Driver, GraphDatabase

logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Result of a Neo4j load operation."""
    success: bool
    files_loaded: int = 0
    nodes_created: int = 0
    relationships_created: int = 0
    error: Optional[str] = None
    failed_files: List[str] = None

    def __post_init__(self):
        if self.failed_files is None:
            self.failed_files = []


class Neo4jLoader:
    """Load data into Neo4j database."""

    # Default Cypher query template for creating Transaction nodes
    # Note: MERGE only uses transaction_id as the unique key
    # Other properties are set via ON CREATE SET to handle null values
    DEFAULT_QUERY_TEMPLATE = """
    CALL {{
    LOAD CSV WITH HEADERS FROM 'file:///{file_name}' AS row
    FIELDTERMINATOR ','
    MATCH (g:Summary_GFCID {{gfcid: row.gfcid}})
    MERGE (t:Transaction {{transaction_id: row.transaction_id}})
    ON CREATE SET
        t.gfcid = row.gfcid,
        t.cagid = row.cagid,
        t.obligor_name = row.obligor_name,
        t.cagid_name = row.cagid_name,
        t.uitid = row.uitid,
        t.netting_id = row.netting_id,
        t.netting_type = row.netting_type,
        t.trade_date = row.trade_date,
        t.cob_date = row.cob_date,
        t.is_stress_eligible = row.is_stress_eligible,
        t.mtm_usd_amount = toFloat(row.mtm_usd_amount),
        t.mtm_local_amount = toFloat(row.mtm_local_amount),
        t.mtm_currency_code = row.mtm_currency_code
    MERGE (t)-[:TRANSACTIONS]->(g)
    }} IN TRANSACTIONS OF 1000 ROWS
    """

    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        database: str,
        query_template: Optional[str] = None
    ):
        """
        Initialize the Neo4j loader.

        Args:
            uri: Neo4j connection URI
            user: Database username
            password: Database password
            database: Database name
            query_template: Custom Cypher query template (optional)
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.query_template = query_template or self.DEFAULT_QUERY_TEMPLATE
        self.driver: Optional[Driver] = None

    def connect(self) -> bool:
        """Establish connection to Neo4j."""
        try:
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
                database=self.database
            )
            self.driver.verify_connectivity()
            logger.info("Successfully connected to Neo4j database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            return False

    def close(self) -> None:
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")

    def ensure_constraints(self) -> bool:
        """Create necessary constraints and indexes."""
        try:
            with self.driver.session(database=self.database) as session:
                # Create constraint for Summary_GFCID
                session.run(
                    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Summary_GFCID) REQUIRE n.gfcid IS UNIQUE"
                )
                # Create index for Transaction
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.transaction_id)"
                )
                # Create index for gfcid on Transaction
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.gfcid)"
                )
                logger.info("Neo4j constraints and indexes ensured")
                return True
        except Exception as e:
            logger.error(f"Failed to create constraints: {e}")
            return False

    def ensure_summary_nodes(self, gfcids: Iterable[str]) -> int:
        """
        Ensure Summary_GFCID nodes exist for the given GFCIDs.

        Args:
            gfcids: Iterable of GFCID values

        Returns:
            Number of nodes created/ensured
        """
        gfcid_list = list({gid for gid in gfcids if gid})
        if not gfcid_list:
            logger.info("No GFCIDs provided for Summary_GFCID creation")
            return 0

        try:
            with self.driver.session(database=self.database) as session:
                query = """
                UNWIND $gfcids AS gfcid
                MERGE (n:Summary_GFCID {gfcid: gfcid})
                """
                session.run(query, gfcids=gfcid_list)
                logger.info(f"Ensured {len(gfcid_list)} Summary_GFCID nodes exist")
                return len(gfcid_list)
        except Exception as e:
            logger.error(f"Failed to create Summary_GFCID nodes: {e}")
            return 0

    def collect_gfcids_from_file(self, file_path: Path) -> Set[str]:
        """
        Collect unique GFCID values from a CSV file.

        Args:
            file_path: Path to the CSV file

        Returns:
            Set of unique GFCID values
        """
        gfcids: Set[str] = set()
        try:
            with file_path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames or "gfcid" not in reader.fieldnames:
                    logger.warning(f"File {file_path} does not contain 'gfcid' column")
                    return gfcids

                for row in reader:
                    value = (row.get("gfcid") or "").strip()
                    if value:
                        gfcids.add(value)

            logger.debug(f"Collected {len(gfcids)} unique GFCIDs from {file_path}")
            return gfcids
        except Exception as e:
            logger.error(f"Failed to collect GFCIDs from {file_path}: {e}")
            return gfcids

    # Default base path for Neo4j LOAD CSV
    DEFAULT_BASE_PATH = "/mnt/nas/"

    def load_file(self, file_path: Path, base_path: Optional[str] = None) -> LoadResult:
        """
        Load a single CSV file into Neo4j.

        Args:
            file_path: Path to the CSV file
            base_path: Base path to strip from file path for Neo4j LOAD CSV

        Returns:
            LoadResult with status
        """
        try:
            # Use default base path if not provided
            if base_path is None:
                base_path = self.DEFAULT_BASE_PATH

            # Collect GFCIDs and ensure Summary nodes exist
            gfcids = self.collect_gfcids_from_file(file_path)
            self.ensure_summary_nodes(gfcids)

            # Prepare file path for Neo4j LOAD CSV
            relative_path = str(file_path).replace(base_path, "").replace(" ", "%20")

            # Execute the query
            query = self.query_template.format(file_name=relative_path)

            with self.driver.session(database=self.database) as session:
                result = session.run(query)
                summary = result.consume()

                logger.info(
                    f"Loaded {file_path.name}: "
                    f"nodes={summary.counters.nodes_created}, "
                    f"relationships={summary.counters.relationships_created}"
                )

                return LoadResult(
                    success=True,
                    files_loaded=1,
                    nodes_created=summary.counters.nodes_created,
                    relationships_created=summary.counters.relationships_created
                )

        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            return LoadResult(
                success=False,
                error=str(e),
                failed_files=[str(file_path)]
            )

    def load_files(
        self,
        file_paths: List[Path],
        parallel: bool = False,
        max_workers: int = 4,
        base_path: Optional[str] = None
    ) -> LoadResult:
        """
        Load multiple CSV files into Neo4j.

        Args:
            file_paths: List of file paths to load
            parallel: Whether to load files in parallel
            max_workers: Maximum number of parallel workers
            base_path: Base path to strip from file paths

        Returns:
            LoadResult with aggregated status
        """
        if not file_paths:
            return LoadResult(success=True, files_loaded=0)

        # Ensure constraints exist
        self.ensure_constraints()

        total_nodes = 0
        total_relationships = 0
        failed_files = []

        if parallel and len(file_paths) > 1:
            # Use multiprocessing for parallel loading
            results = self._load_parallel(file_paths, max_workers, base_path)
            for result in results:
                if result.success:
                    total_nodes += result.nodes_created
                    total_relationships += result.relationships_created
                else:
                    failed_files.extend(result.failed_files)
        else:
            # Sequential loading
            for file_path in file_paths:
                result = self.load_file(file_path, base_path)
                if result.success:
                    total_nodes += result.nodes_created
                    total_relationships += result.relationships_created
                else:
                    failed_files.extend(result.failed_files)

        success = len(failed_files) == 0
        return LoadResult(
            success=success,
            files_loaded=len(file_paths) - len(failed_files),
            nodes_created=total_nodes,
            relationships_created=total_relationships,
            failed_files=failed_files,
            error=f"{len(failed_files)} files failed" if failed_files else None
        )

    def _load_parallel(
        self,
        file_paths: List[Path],
        max_workers: int,
        base_path: str
    ) -> List[LoadResult]:
        """Load files in parallel using multiprocessing."""
        # Note: Each worker needs its own connection
        # For simplicity, we'll use sequential loading within this implementation
        # A full parallel implementation would use a worker function
        results = []
        for file_path in file_paths:
            result = self.load_file(file_path, base_path)
            results.append(result)
        return results


def create_loader_from_settings(settings: Dict[str, Any]) -> Optional[Neo4jLoader]:
    """
    Create a Neo4jLoader from settings dictionary.

    Args:
        settings: Settings dictionary (from settings.yaml)

    Returns:
        Neo4jLoader instance or None if configuration is missing
    """
    neo4j_config = settings.get("DATABASES", {}).get("neo4j", {})

    uri = neo4j_config.get("NE04J_URI") or neo4j_config.get("NEO4J_URI")
    user = neo4j_config.get("USER")
    password = neo4j_config.get("PASSWORD")
    database = neo4j_config.get("DATABASE")

    if not all([uri, user, password, database]):
        logger.error("Missing Neo4j configuration in settings")
        return None

    loader = Neo4jLoader(uri, user, password, database)
    if loader.connect():
        return loader
    return None
