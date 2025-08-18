#!/usr/bin/env python3
"""
Simple Batch Patient Filter
Filters FHIR resources for multiple patients into single NDJSON files.
"""

import json
import argparse
import logging
import tempfile
import shutil
import subprocess
from typing import Set, List
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SimpleBatchFilter:
    """Simple batch filtering for multiple patients."""

    def __init__(
        self, patient_ids: List[str], fhir_dir: str = "./input_data/fhir", max_workers: int = None
    ):
        self.patient_ids = set(patient_ids)
        self.fhir_dir = Path(fhir_dir)
        self.temp_dir = None
        # Default to using most cores, leave 1 for system
        self.max_workers = max_workers or max(1, multiprocessing.cpu_count() - 1)
        logger.info(
            f"Initialized simple batch filter for {len(patient_ids)} patients using {self.max_workers} worker threads"
        )

    def create_temp_directory(self) -> Path:
        """Create a temporary directory for filtered files."""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="simple_batch_"))
        logger.info(f"Created temporary directory: {self.temp_dir}")
        return self.temp_dir

    def cleanup_temp_directory(self):
        """Clean up the temporary directory."""
        if self.temp_dir and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temporary directory: {self.temp_dir}")

    def grep_filter_file(
        self, input_file: Path, output_file: Path, patterns: List[str]
    ) -> int:
        """Use grep to filter a file and write matching lines to output."""
        if not input_file.exists():
            logger.warning(f"File not found: {input_file}")
            return 0

        try:
            # Create pattern file
            pattern_file = self.temp_dir / f"patterns_{input_file.stem}.txt"
            with open(pattern_file, "w") as f:
                for pattern in patterns:
                    f.write(f"{pattern}\n")

            # Use grep to filter and write directly to output
            cmd = ["zgrep", "-F", "-f", str(pattern_file), str(input_file)]

            with open(output_file, "w") as out_f:
                result = subprocess.run(
                    cmd, stdout=out_f, stderr=subprocess.PIPE, text=True
                )

            # Clean up pattern file
            pattern_file.unlink()

            if result.returncode == 0:
                # Count lines in output file
                with open(output_file, "r") as f:
                    count = sum(1 for _ in f)
                return count
            elif result.returncode == 1:
                # No matches found - remove empty file
                if output_file.exists():
                    output_file.unlink()
                return 0
            else:
                logger.warning(f"grep failed for {input_file}: {result.stderr}")
                if output_file.exists():
                    output_file.unlink()
                return 0

        except subprocess.TimeoutExpired:
            logger.error(f"grep timeout for {input_file} s")
            if output_file.exists():
                output_file.unlink()
            return 0
        except Exception as e:
            logger.error(f"Error filtering {input_file}: {e}")
            if output_file.exists():
                output_file.unlink()
            return 0

    def filter_single_file(
        self, fhir_file: Path, patterns: List[str]
    ) -> tuple[Path, int]:
        """Filter a single FHIR file. Returns (output_file, count)."""
        if not fhir_file.exists():
            logger.warning(f"File not found: {fhir_file}")
            return None, 0

        file_size_gb = fhir_file.stat().st_size / (1024**3)
        logger.info(f"Processing {fhir_file.name} ({file_size_gb:.1f}GB)...")
        start_time = time.time()

        # Create output filename (remove .gz extension)
        output_file = self.temp_dir / fhir_file.name.replace(".gz", "")

        count = self.grep_filter_file(fhir_file, output_file, patterns)

        elapsed = time.time() - start_time
        if count > 0:
            logger.info(
                f"  → {fhir_file.name}: {count} records filtered in {elapsed:.2f}s ({file_size_gb/elapsed*60:.1f}GB/min)"
            )
        else:
            logger.info(f"  → {fhir_file.name}: No matches found in {elapsed:.2f}s")

        return output_file, count

    def filter_all_files(self) -> int:
        """Filter all FHIR files for the target patients using multithreading."""
        if not self.temp_dir:
            self.create_temp_directory()

        # Get all NDJSON files in the fhir directory
        fhir_files = list(self.fhir_dir.glob("*.ndjson.gz"))

        if not fhir_files:
            logger.error("No NDJSON.gz files found in fhir directory")
            return 0

        logger.info(
            f"Found {len(fhir_files)} FHIR files to process with {self.max_workers} threads"
        )

        total_filtered = 0
        patterns = list(self.patient_ids)

        # Use ThreadPoolExecutor to process files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self.filter_single_file, fhir_file, patterns): fhir_file
                for fhir_file in fhir_files
            }

            # Process completed tasks
            for future in as_completed(future_to_file):
                fhir_file = future_to_file[future]
                try:
                    output_file, count = future.result()
                    total_filtered += count
                except Exception as e:
                    logger.error(f"Error processing {fhir_file.name}: {e}")

        return total_filtered

    def create_summary(self, total_filtered: int):
        """Create a summary of filtered resources."""
        summary_file = self.temp_dir / "SUMMARY.txt"

        with open(summary_file, "w") as f:
            f.write("SIMPLE BATCH FILTERING SUMMARY\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Target patients: {len(self.patient_ids)}\n")
            f.write(
                f"Patient IDs: {', '.join(sorted(list(self.patient_ids)[:5]))}{'...' if len(self.patient_ids) > 5 else ''}\n\n"
            )

            total_resources = 0
            ndjson_files = list(self.temp_dir.glob("*.ndjson"))

            for ndjson_file in sorted(ndjson_files):
                with open(ndjson_file, "r") as rf:
                    count = sum(1 for _ in rf)
                f.write(f"{ndjson_file.name:<40} {count:>8} resources\n")
                total_resources += count

            f.write("-" * 50 + "\n")
            f.write(f"{'TOTAL':<40} {total_resources:>8} resources\n")

        logger.info(f"Summary written to {summary_file}")


def read_patient_list(filepath: str) -> List[str]:
    """Read patient IDs from a file (one per line)."""
    patient_ids = []
    with open(filepath, "r") as f:
        for line in f:
            patient_id = line.strip()
            if patient_id and not patient_id.startswith("#"):  # Skip comments
                patient_ids.append(patient_id)
    return patient_ids


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Simple batch filter FHIR resources for multiple patients"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--patient-list", help="File containing patient IDs (one per line)"
    )
    group.add_argument(
        "--patients", nargs="+", help="Patient IDs as command line arguments"
    )

    parser.add_argument(
        "--fhir-dir",
        default="./input_data/fhir",
        help="Directory containing FHIR NDJSON files (default: ./input_data/fhir)",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep temporary directory after completion",
    )
    parser.add_argument("--output-dir", help="Copy filtered files to this directory")
    parser.add_argument(
        "--threads",
        type=int,
        help=f"Number of worker threads (default: {max(1, multiprocessing.cpu_count() - 1)})",
    )

    args = parser.parse_args()

    # Get patient IDs
    if args.patient_list:
        patient_ids = read_patient_list(args.patient_list)
        logger.info(f"Read {len(patient_ids)} patient IDs from {args.patient_list}")
    else:
        patient_ids = args.patients

    if not patient_ids:
        logger.error("No patient IDs provided")
        return

    start_time = time.time()
    logger.info(f"Starting simple batch filter for {len(patient_ids)} patients")

    # Create filter instance
    filter_instance = SimpleBatchFilter(patient_ids, args.fhir_dir, args.threads)

    try:
        # Filter resources
        total_filtered = filter_instance.filter_all_files()

        if total_filtered == 0:
            logger.warning("No resources found for any of the specified patients")
            return

        # Create summary
        filter_instance.create_summary(total_filtered)

        total_time = time.time() - start_time
        logger.info(f"Simple batch filtering completed in {total_time:.2f} seconds")
        logger.info(f"Filtered {total_filtered} total resources")

        # Copy to output directory if specified
        if args.output_dir:
            output_path = Path(args.output_dir)
            output_path.mkdir(exist_ok=True)

            for ndjson_file in filter_instance.temp_dir.glob("*.ndjson"):
                shutil.copy2(ndjson_file, output_path)

            # Also copy summary
            summary_file = filter_instance.temp_dir / "SUMMARY.txt"
            if summary_file.exists():
                shutil.copy2(summary_file, output_path)

            logger.info(f"Filtered files copied to: {output_path}")

        if not args.output_dir:
            logger.info(f"Filtered files are in: {filter_instance.temp_dir}")

    finally:
        # Always preserve temp directory for inspection
        logger.info(f"Temporary directory preserved at: {filter_instance.temp_dir}")
        # Note: Temp files are kept for manual inspection and cleanup


if __name__ == "__main__":
    main()
