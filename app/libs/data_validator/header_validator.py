import csv
import os

import pandas as pd
from neo4j import GraphDatabase

from app.config.settings import (
    DEFAULT_SETTINGS_PATH,
    Settings,
    SettingsError,
    load_settings,
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


class Neo4jCSVUtils:
    def __init__(self, settings: Settings | str = DEFAULT_SETTINGS_PATH):
        self.domain_map = {
            "API": "ApiDomain",
            "CALCULATION": "CalculationDomain",
            "CORE": "CoreDomain",
            "CURATED": "CuratedDomain",
            "FEED": "FeedDomain",
            "REPORT": "ReportDomain",
            "REPORT_CATALOG": "ReportCatalogDomain",
            "SCHEDULE": "ScheduleDomain",
        }

        if isinstance(settings, Settings):
            self._settings = settings
        else:
            try:
                self._settings = load_settings(settings)
            except SettingsError as exc:
                raise ValueError(f"Unable to load settings: {exc}") from exc

        databases = self._settings.DATABASES
        if "neo4j" not in databases:
            raise ValueError("settings must define DATABASES.neo4j for Neo4j access")

        neo4j_cfg = databases["neo4j"]
        required_keys = {"NEO4J_URI", "USER", "PASSWORD"}
        missing = required_keys - neo4j_cfg.keys()
        if missing:
            missing_keys = ", ".join(sorted(missing))
            raise ValueError(f"Neo4j configuration missing keys: {missing_keys}")

        self.driver = GraphDatabase.driver(
            neo4j_cfg["NEO4J_URI"],
            auth=(neo4j_cfg["USER"], neo4j_cfg["PASSWORD"]),
        )
        self.database = neo4j_cfg.get("DATABASE")

    def query(self, cypher):
        # Connect and run cypher query, return pandas DataFrame
        with self.driver.session(database=self.database) as session:
            result = session.run(cypher)
            records = list(result)
            if records:
                df = pd.DataFrame([r.data() for r in records])
            else:
                df = pd.DataFrame()
        return df

    def get_domain_file_list(self, domain_name=None):

        domain_label = self.domain_map.get(domain_name.upper())

        with self.driver.session(database=self.database) as session:
            result = session.run(
                f"""
                        MATCH (n:{domain_label})
                        RETURN n.name AS name
                    """
            )
            records = list(result)
            if records:
                df = pd.DataFrame([r.data() for r in records])
            else:
                df = pd.DataFrame()
        return df

    def get_csv_headers(self, dir_path):
        header_dict = {}
        for fname in os.listdir(dir_path):
            if fname.lower().endswith(".csv"):
                full_path = os.path.join(dir_path, fname)
                try:
                    with open(full_path, "r", newline="", encoding="utf-8") as f:
                        reader = csv.reader(f)
                        headers = next(reader)
                        header_dict[full_path] = {"header": headers}
                except Exception as e:
                    header_dict[full_path] = {"header": [], "error": str(e)}
        return header_dict


if __name__ == "__main__":
    # Assuming settings.yaml is in the working directory with the expected structure.
    # Example usage:
    utils = Neo4jCSVUtils()
    df = utils.query("MATCH (n:ReportDomain) RETURN n LIMIT 10")
    print(df)
    headers = utils.get_csv_headers("./data/")
    print(headers)
