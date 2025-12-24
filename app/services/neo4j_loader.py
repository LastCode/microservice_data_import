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

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
CYPHER_DIR = PROJECT_ROOT / "conf" / "cypher"

# Cypher query file mapping
CYPHER_FILES = {
    "cagid": "cypher_02_summary_cagid.cql",
    "gfcid": "cypher_03_summary_gfcid.cql",
    "nettingid": "cypher_04_summary_nettingid.cql",
    "relationships": "cypher_05_create_relationships.cql",
}


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
    LOAD CSV WITH HEADERS FROM 'file://{file_name}' AS row
    FIELDTERMINATOR ','
    WITH row WHERE row.transaction_id IS NOT NULL AND row.gfcid IS NOT NULL
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
        """Create necessary constraints and indexes for all node types."""
        try:
            with self.driver.session(database=self.database) as session:
                # ========== Constraints ==========
                session.run(
                    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Summary_GFCID) REQUIRE n.gfcid IS UNIQUE"
                )

                # ========== Transaction Indexes ==========
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.transaction_id)"
                )
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.cagid)"
                )
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.gfcid)"
                )
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.netting_id)"
                )
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (n:Transaction) ON (n.cob_date)"
                )

                # ========== Summary_CAGID Indexes ==========
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (s:Summary_CAGID) ON (s.cagid)"
                )
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (s:Summary_CAGID) ON (s.cob_date)"
                )

                # ========== Summary_GFCID Indexes ==========
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (s:Summary_GFCID) ON (s.gfcid)"
                )
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (s:Summary_GFCID) ON (s.cob_date)"
                )

                # ========== Summary_NETTINGID Indexes ==========
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (s:Summary_NETTINGID) ON (s.netting_id)"
                )
                session.run(
                    "CREATE INDEX IF NOT EXISTS FOR (s:Summary_NETTINGID) ON (s.cob_date)"
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
            # Use full absolute path for local Neo4j server
            full_path = str(file_path).replace(" ", "%20")

            # Execute the query
            query = self.query_template.format(file_name=full_path)

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
        base_path: Optional[str] = None,
        run_post_processing: bool = False,
        cob_date: Optional[str] = None
    ) -> LoadResult:
        """
        Load multiple CSV files into Neo4j.

        Args:
            file_paths: List of file paths to load
            parallel: Whether to load files in parallel
            max_workers: Maximum number of parallel workers
            base_path: Base path to strip from file paths
            run_post_processing: Whether to run aggregation and create relationships after loading
            cob_date: COB date for filtering post-processing (format: YYYY-MM-DD)

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

        # Run post-processing if requested and load was successful
        if run_post_processing and len(failed_files) == 0:
            logger.info("Running post-load processing (aggregation & relationships)...")
            post_results = self.run_post_load_processing(cob_date=cob_date)
            logger.info(f"Post-processing results: {post_results}")

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

    def run_cypher_file(self, file_path: Path) -> bool:
        """
        Execute a Cypher query from an external .cql file.

        Args:
            file_path: Path to the .cql file

        Returns:
            True if successful, False otherwise
        """
        try:
            if not file_path.exists():
                logger.error(f"Cypher file not found: {file_path}")
                return False

            query = file_path.read_text(encoding="utf-8")

            # Remove comments and empty lines for cleaner execution
            lines = []
            for line in query.split("\n"):
                stripped = line.strip()
                if stripped and not stripped.startswith("//"):
                    lines.append(line)
            clean_query = "\n".join(lines)

            if not clean_query.strip():
                logger.warning(f"Empty query in file: {file_path}")
                return True

            with self.driver.session(database=self.database) as session:
                result = session.run(clean_query)
                summary = result.consume()
                logger.info(
                    f"Executed {file_path.name}: "
                    f"nodes_created={summary.counters.nodes_created}, "
                    f"relationships_created={summary.counters.relationships_created}"
                )
                return True

        except Exception as e:
            logger.error(f"Failed to execute {file_path}: {e}")
            return False

    def run_aggregation(
        self,
        cob_date: Optional[str] = None,
        aggregate_types: Optional[List[str]] = None,
        cypher_dir: Optional[Path] = None
    ) -> Dict[str, bool]:
        """
        Run aggregation queries from .cql files to create Summary nodes.

        Args:
            cob_date: Optional COB date filter (format: YYYY-MM-DD) - NOT USED when loading from files
            aggregate_types: List of aggregation types to run ['cagid', 'gfcid', 'nettingid']
                           If None, runs all aggregations
            cypher_dir: Directory containing .cql files (defaults to conf/cypher/)

        Returns:
            Dict with aggregation type as key and success status as value
        """
        if aggregate_types is None:
            aggregate_types = ["cagid", "gfcid", "nettingid"]

        if cypher_dir is None:
            cypher_dir = CYPHER_DIR

        results = {}

        for agg_type in aggregate_types:
            try:
                # Get the .cql file for this aggregation type
                cql_filename = CYPHER_FILES.get(agg_type)
                if not cql_filename:
                    logger.warning(f"No .cql file configured for aggregation type: {agg_type}")
                    results[agg_type] = False
                    continue

                cql_path = cypher_dir / cql_filename

                if not cql_path.exists():
                    logger.error(f"Cypher file not found: {cql_path}")
                    results[agg_type] = False
                    continue

                logger.info(f"Running aggregation from file: {cql_path}")
                success = self.run_cypher_file(cql_path)
                results[agg_type] = success

            except Exception as e:
                logger.error(f"Aggregation failed for {agg_type}: {e}")
                results[agg_type] = False

        return results

    def create_relationships(self, cob_date: Optional[str] = None, cypher_dir: Optional[Path] = None) -> bool:
        """
        Create relationships between Summary nodes by loading from .cql file.

        Args:
            cob_date: Optional COB date filter - NOT USED when loading from files
            cypher_dir: Directory containing .cql files (defaults to conf/cypher/)

        Returns:
            True if successful, False otherwise
        """
        if cypher_dir is None:
            cypher_dir = CYPHER_DIR

        cql_filename = CYPHER_FILES.get("relationships")
        if not cql_filename:
            logger.error("No .cql file configured for relationships")
            return False

        cql_path = cypher_dir / cql_filename

        if not cql_path.exists():
            logger.error(f"Cypher file not found: {cql_path}")
            return False

        logger.info(f"Creating relationships from file: {cql_path}")
        return self.run_cypher_file(cql_path)

    def run_post_load_processing(
        self,
        cob_date: Optional[str] = None,
        run_aggregation: bool = True,
        run_relationships: bool = True
    ) -> Dict[str, Any]:
        """
        Run all post-load processing (aggregation and relationships).

        Args:
            cob_date: Optional COB date filter
            run_aggregation: Whether to run aggregation queries
            run_relationships: Whether to create relationships

        Returns:
            Dict with processing results
        """
        results = {
            "aggregation": {},
            "relationships": False
        }

        if run_aggregation:
            logger.info("Starting aggregation processing...")
            results["aggregation"] = self.run_aggregation(cob_date=cob_date)

        if run_relationships:
            logger.info("Creating relationships between Summary nodes...")
            results["relationships"] = self.create_relationships(cob_date=cob_date)

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
