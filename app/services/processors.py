"""Data processors for transforming and splitting files."""
from __future__ import annotations

import csv
import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import shutil

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

# Check if 'cut' command is available (Unix/Linux/macOS)
HAS_CUT = shutil.which("cut") is not None

logger = logging.getLogger(__name__)


@dataclass
class ProcessResult:
    """Result of a processing operation."""
    success: bool
    output_path: Optional[Path] = None
    output_paths: Optional[List[Path]] = None
    error: Optional[str] = None
    rows_processed: int = 0


class ColumnCutter:
    """Cut specific columns from a delimited file."""

    def __init__(self, column_config: Dict[str, Any]):
        """
        Initialize the column cutter.

        Args:
            column_config: Configuration from column_map.yaml for the domain
        """
        self.column_config = column_config
        self.delimiter = column_config.get("delimiter", "\x01")
        self.output_delimiter = column_config.get("output_delimiter", ",")
        self.has_header = column_config.get("has_header", False)
        self.columns_to_extract = column_config.get("required_columns_by_index", "")
        self.column_names = column_config.get("column_names", [])

    def process(self, source_path: Path, destination_path: Path) -> ProcessResult:
        """
        Cut columns from source file and save to destination.

        Args:
            source_path: Path to the source file
            destination_path: Path for the output file

        Returns:
            ProcessResult with status and output path
        """
        try:
            if not source_path.exists():
                return ProcessResult(
                    success=False,
                    error=f"Source file not found: {source_path}"
                )

            # Ensure destination directory exists
            destination_path.parent.mkdir(parents=True, exist_ok=True)

            # Use cut command for efficiency with large files (if available)
            if self.columns_to_extract:
                if HAS_CUT:
                    result = self._cut_with_command(source_path, destination_path)
                else:
                    logger.info("'cut' command not available, using Python fallback")
                    result = self._cut_with_python(source_path, destination_path)
            else:
                # If no columns specified, copy as-is with delimiter conversion
                result = self._convert_delimiter(source_path, destination_path)

            return result

        except Exception as e:
            logger.error(f"Column cutting failed: {e}")
            return ProcessResult(success=False, error=str(e))

    def _cut_with_command(self, source_path: Path, destination_path: Path) -> ProcessResult:
        """Use the cut command for efficient column extraction."""
        try:
            # Build cut command
            # cut -d $'\x01' -f 2,4,5,6 source > dest
            delimiter_escaped = self.delimiter
            if self.delimiter == "\x01":
                delimiter_escaped = "$'\\x01'"

            cmd = f"cut -d {delimiter_escaped} -f {self.columns_to_extract} '{source_path}'"

            # If we need to change the output delimiter
            if self.output_delimiter != self.delimiter:
                # Convert delimiter using tr or sed
                if self.output_delimiter == ",":
                    cmd += f" | tr '{self.delimiter}' ','"
                else:
                    cmd += f" | sed 's/{self.delimiter}/{self.output_delimiter}/g'"

            cmd += f" > '{destination_path}'"

            # Execute the command
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=600
            )

            if result.returncode != 0:
                return ProcessResult(
                    success=False,
                    error=f"Cut command failed: {result.stderr}"
                )

            # Count rows processed
            row_count = self._count_lines(destination_path)

            # Add header if column names are defined
            if self.column_names and not self.has_header:
                self._add_header(destination_path)

            logger.info(f"Successfully cut columns from {source_path} to {destination_path} ({row_count} rows)")
            return ProcessResult(
                success=True,
                output_path=destination_path,
                rows_processed=row_count
            )

        except subprocess.TimeoutExpired:
            return ProcessResult(success=False, error="Cut operation timed out")
        except Exception as e:
            return ProcessResult(success=False, error=str(e))

    def _cut_with_python(self, source_path: Path, destination_path: Path) -> ProcessResult:
        """Pure Python fallback for column extraction (used when 'cut' is unavailable)."""
        try:
            # Parse column indices (1-based in config, convert to 0-based)
            col_indices = [int(c) - 1 for c in self.columns_to_extract.split(",")]
            row_count = 0

            with source_path.open("r", encoding="utf-8", errors="replace") as src:
                with destination_path.open("w", encoding="utf-8") as dst:
                    for line in src:
                        fields = line.rstrip("\n\r").split(self.delimiter)
                        selected = [fields[i] if i < len(fields) else "" for i in col_indices]
                        dst.write(self.output_delimiter.join(selected) + "\n")
                        row_count += 1

            # Add header if column names are defined
            if self.column_names and not self.has_header:
                self._add_header(destination_path)

            logger.info(f"Successfully cut columns (Python) from {source_path} to {destination_path} ({row_count} rows)")
            return ProcessResult(
                success=True,
                output_path=destination_path,
                rows_processed=row_count
            )

        except Exception as e:
            return ProcessResult(success=False, error=str(e))

    def _convert_delimiter(self, source_path: Path, destination_path: Path) -> ProcessResult:
        """Convert file delimiter without cutting columns."""
        try:
            row_count = 0
            with source_path.open("r", encoding="utf-8", errors="replace") as src:
                with destination_path.open("w", encoding="utf-8") as dst:
                    for line in src:
                        converted = line.replace(self.delimiter, self.output_delimiter)
                        dst.write(converted)
                        row_count += 1

            logger.info(f"Converted delimiter in {source_path} ({row_count} rows)")
            return ProcessResult(
                success=True,
                output_path=destination_path,
                rows_processed=row_count
            )
        except Exception as e:
            return ProcessResult(success=False, error=str(e))

    def _count_lines(self, file_path: Path) -> int:
        """Count lines in a file."""
        try:
            result = subprocess.run(
                ["wc", "-l", str(file_path)],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                return int(result.stdout.strip().split()[0])
        except Exception:
            pass
        return 0

    def _add_header(self, file_path: Path) -> None:
        """Add header row to a file."""
        try:
            header = self.output_delimiter.join(self.column_names)
            temp_path = file_path.with_suffix(".tmp")

            with file_path.open("r") as src:
                with temp_path.open("w") as dst:
                    dst.write(header + "\n")
                    dst.write(src.read())

            temp_path.replace(file_path)
            logger.debug(f"Added header to {file_path}")
        except Exception as e:
            logger.warning(f"Failed to add header: {e}")


class FileSplitter:
    """Split a file by a key column into multiple files."""

    def __init__(self, column_config: Dict[str, Any]):
        """
        Initialize the file splitter.

        Args:
            column_config: Configuration from column_map.yaml for the domain
        """
        self.column_config = column_config
        self.delimiter = column_config.get("output_delimiter", ",")
        self.split_by_column = column_config.get("split_by_column", "gfcid")
        self.split_by_column_index = column_config.get("split_by_column_index", 0)
        self.column_names = column_config.get("column_names", [])
        self.chunk_size = column_config.get("chunk_size", 50000)

    def process(
        self,
        source_path: Path,
        output_dir: Path,
        output_prefix: str,
        cob_date: str
    ) -> ProcessResult:
        """
        Split a file by key column.

        Args:
            source_path: Path to the source file
            output_dir: Directory for output files
            output_prefix: Prefix for output filenames
            cob_date: COB date for filename

        Returns:
            ProcessResult with list of output files
        """
        try:
            if not source_path.exists():
                return ProcessResult(
                    success=False,
                    error=f"Source file not found: {source_path}"
                )

            # Ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)

            # Use pandas for chunked processing
            output_paths = self._split_with_pandas(
                source_path, output_dir, output_prefix, cob_date
            )

            if not output_paths:
                return ProcessResult(
                    success=False,
                    error="No output files generated"
                )

            logger.info(f"Split {source_path} into {len(output_paths)} files")
            return ProcessResult(
                success=True,
                output_paths=output_paths,
                rows_processed=sum(self._count_lines(p) for p in output_paths)
            )

        except Exception as e:
            logger.error(f"File splitting failed: {e}")
            return ProcessResult(success=False, error=str(e))

    def _split_with_pandas(
        self,
        source_path: Path,
        output_dir: Path,
        output_prefix: str,
        cob_date: str
    ) -> List[Path]:
        """Split file using pandas for chunked processing."""
        if not HAS_PANDAS:
            return self._split_with_csv(source_path, output_dir, output_prefix, cob_date)

        output_paths = []
        file_handles: Dict[str, Any] = {}

        try:
            # Determine if file has header
            has_header = bool(self.column_names)
            header = 0 if has_header else None
            names = self.column_names if not has_header else None

            # Read in chunks
            reader = pd.read_csv(
                source_path,
                delimiter=self.delimiter,
                header=header,
                names=names,
                chunksize=self.chunk_size,
                dtype=str,
                na_filter=False
            )

            for chunk_num, chunk in enumerate(reader):
                logger.debug(f"Processing chunk {chunk_num + 1} with {len(chunk)} rows")

                # Get the split column name
                if self.column_names:
                    split_col = self.split_by_column
                else:
                    split_col = chunk.columns[self.split_by_column_index]

                # Group by the split column
                for group_key, group_df in chunk.groupby(split_col):
                    # Sanitize the group key for filename
                    safe_key = str(group_key).replace("/", "_").replace("\\", "_")
                    output_filename = f"{output_prefix}{safe_key}.csv"
                    output_path = output_dir / output_filename

                    # Check if we need to write header
                    file_exists = output_path.exists()

                    # Append to file
                    group_df.to_csv(
                        output_path,
                        mode="a",
                        header=not file_exists,
                        index=False
                    )

                    if output_path not in output_paths:
                        output_paths.append(output_path)

            return output_paths

        except Exception as e:
            logger.error(f"Pandas split failed: {e}")
            raise
        finally:
            # Close any open file handles
            for handle in file_handles.values():
                try:
                    handle.close()
                except Exception:
                    pass

    def _count_lines(self, file_path: Path) -> int:
        """Count lines in a file (excluding header)."""
        try:
            result = subprocess.run(
                ["wc", "-l", str(file_path)],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                count = int(result.stdout.strip().split()[0])
                return max(0, count - 1)  # Subtract header
        except Exception:
            pass
        return 0

    def _split_with_csv(
        self,
        source_path: Path,
        output_dir: Path,
        output_prefix: str,
        cob_date: str
    ) -> List[Path]:
        """Split file using standard library csv module (fallback when pandas unavailable)."""
        output_paths = []
        file_handles: Dict[str, Any] = {}

        try:
            with source_path.open("r", newline="", encoding="utf-8", errors="replace") as f:
                # Check if file has header
                has_header = bool(self.column_names)

                if has_header:
                    reader = csv.DictReader(f, delimiter=self.delimiter)
                    fieldnames = reader.fieldnames
                    split_col = self.split_by_column
                else:
                    reader = csv.reader(f, delimiter=self.delimiter)
                    fieldnames = None
                    split_col_idx = self.split_by_column_index

                for row in reader:
                    # Get the split key
                    if has_header:
                        key = row.get(split_col, "unknown")
                    else:
                        key = row[split_col_idx] if len(row) > split_col_idx else "unknown"

                    # Sanitize key for filename
                    safe_key = str(key).replace("/", "_").replace("\\", "_")
                    output_filename = f"{output_prefix}{safe_key}.csv"
                    output_path = output_dir / output_filename

                    # Get or create file handle
                    if safe_key not in file_handles:
                        file_exists = output_path.exists()
                        handle = output_path.open("a", newline="", encoding="utf-8")
                        writer = csv.writer(handle)

                        # Write header if needed
                        if not file_exists and self.column_names:
                            writer.writerow(self.column_names)

                        file_handles[safe_key] = {"handle": handle, "writer": writer}

                        if output_path not in output_paths:
                            output_paths.append(output_path)

                    # Write the row
                    if has_header:
                        file_handles[safe_key]["writer"].writerow(row.values())
                    else:
                        file_handles[safe_key]["writer"].writerow(row)

            return output_paths

        except Exception as e:
            logger.error(f"CSV split failed: {e}")
            raise
        finally:
            # Close all file handles
            for data in file_handles.values():
                try:
                    data["handle"].close()
                except Exception:
                    pass


class DataProcessor:
    """Orchestrate the data processing pipeline."""

    # Default dropbox directory (can be overridden via settings)
    DEFAULT_DROPBOX_DIR = "/mnt/nas"

    def __init__(self, column_config: Dict[str, Any], dropbox_dir: Optional[str] = None):
        """
        Initialize the data processor.

        Args:
            column_config: Configuration from column_map.yaml for the domain
            dropbox_dir: Base directory for output files (default: /mnt/nas)
        """
        self.column_config = column_config
        self.dropbox_dir = dropbox_dir or self.DEFAULT_DROPBOX_DIR
        self.cutter = ColumnCutter(column_config)
        self.splitter = FileSplitter(column_config)

    def process_file(
        self,
        source_path: Path,
        cob_date: str,
        skip_cut: bool = False,
        skip_split: bool = False
    ) -> Dict[str, ProcessResult]:
        """
        Process a file through the full pipeline.

        Args:
            source_path: Path to the source file
            cob_date: COB date for output paths
            skip_cut: Skip the column cutting step
            skip_split: Skip the file splitting step

        Returns:
            Dictionary of results for each step
        """
        results = {}

        # Get output paths from config, using dropbox_dir as base
        default_processed_dir = f"{self.dropbox_dir}/{cob_date}"
        default_split_dir = f"{self.dropbox_dir}/{cob_date}/split"

        processed_dir = Path(
            self.column_config.get("processed_output_dir", default_processed_dir)
            .format(cob_date=cob_date, dropbox_dir=self.dropbox_dir)
        )
        processed_file = self.column_config.get(
            "processed_output_file",
            "processed_{cob_date}.dat"
        ).format(cob_date=cob_date)
        split_dir = Path(
            self.column_config.get("split_output_dir", default_split_dir)
            .format(cob_date=cob_date, dropbox_dir=self.dropbox_dir)
        )
        split_prefix = self.column_config.get(
            "split_output_prefix",
            "split_{cob_date}-"
        ).format(cob_date=cob_date)

        current_file = source_path

        # Step 1: Cut columns
        if not skip_cut:
            cut_output = processed_dir / processed_file
            results["cut"] = self.cutter.process(current_file, cut_output)
            if not results["cut"].success:
                return results
            current_file = cut_output
        else:
            results["cut"] = ProcessResult(success=True, output_path=current_file)

        # Step 2: Split by key column
        if not skip_split:
            results["split"] = self.splitter.process(
                current_file, split_dir, split_prefix, cob_date
            )
        else:
            results["split"] = ProcessResult(
                success=True,
                output_paths=[current_file]
            )

        return results
