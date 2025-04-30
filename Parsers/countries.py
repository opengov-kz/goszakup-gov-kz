
import argparse
import logging
import os
import pandas as pd
import requests
import time
from pathlib import Path
import tempfile

def setup_logger(log_filename: str = "countries.log") -> logging.Logger:
    """Configure and return a logger."""
    logger = logging.getLogger("CountriesParser")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

class CountriesFetcher:
    """Class to interact with the Goszakup API and retrieve country list."""

    BASE_URL = "https://ows.goszakup.gov.kz/v3/refs/ref_countries"

    def __init__(self, token: str, logger: logging.Logger):
        self.token = token
        self.logger = logger
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.params = {"limit": 100}

    def get_countries(self) -> list:
        """Fetch countries from the API."""
        self.logger.info("Sending request to fetch country list...")
        all_countries = []
        search_after = None

        while True:
            try:
                if search_after:
                    self.params.update({"page": "next", "search_after": search_after})

                response = requests.get(
                    self.BASE_URL,
                    headers=self.headers,
                    params=self.params,
                    timeout=20
                )
                response.raise_for_status()
                payload = response.json()

                items = payload.get("items", [])
                if not items:
                    self.logger.info("No more countries to fetch.")
                    break

                for item in items:
                    all_countries.append({
                        "code": item.get("code"),
                        "name_kz": item.get("name_kz"),
                        "code_2": item.get("code_2"),
                        "name_ru": item.get("name_ru"),
                        "code_3": item.get("code_3")
                    })

                search_after = items[-1]["code"]
                if not payload.get("next_page"):
                    break

                time.sleep(2)

            except requests.RequestException as e:
                self.logger.error(f"API request failed: {e}")
                raise

        self.logger.info(f"Successfully fetched {len(all_countries)} countries.")
        return all_countries

class CSVWriter:
    """Class to handle CSV writing operations."""

    def __init__(self, output_path: str, logger: logging.Logger):
        self.output_path = output_path
        self.logger = logger
        self.temp_dir = tempfile.mkdtemp()

    def write(self, data: list):
        """Write data to CSV."""
        if not data:
            self.logger.warning("No data provided for CSV export.")
            return

        df = pd.DataFrame(data)
        try:
            df.to_csv(
                self.output_path,
                index=False,
                encoding="utf-8-sig",
                sep="|",
                quotechar="'",
                escapechar="\\"
            )
            self.logger.info(f"Data successfully written to {self.output_path}")
        except Exception as e:
            self.logger.error(f"Failed to write CSV: {e}")
            raise

    def save_checkpoint(self, data: list):
        """Save temporary CSV checkpoint."""
        checkpoint_path = os.path.join(self.temp_dir, f"checkpoint_{int(time.time())}.csv")
        df = pd.DataFrame(data)
        try:
            df.to_csv(
                checkpoint_path,
                index=False,
                encoding="utf-8-sig",
                sep="|",
                quotechar="'",
                escapechar="\\"
            )
            self.logger.info(f"Checkpoint saved at {checkpoint_path}")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")

    def cleanup(self):
        """Remove temporary files."""
        for temp_file in Path(self.temp_dir).glob("*.csv"):
            try:
                temp_file.unlink()
            except Exception as e:
                self.logger.error(f"Error deleting temp file: {e}")
        try:
            os.rmdir(self.temp_dir)
            self.logger.info("Temporary directory cleaned up.")
        except Exception as e:
            self.logger.error(f"Failed to remove temporary directory: {e}")

def parse_args():
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Download country list from Goszakup API and save to CSV.")
    parser.add_argument("--token", required=True, help="Access token for Goszakup API")
    parser.add_argument("--output", default="countries.csv", help="Path to output CSV file (default: countries.csv)")
    return parser.parse_args()

def main():
    args = parse_args()
    logger = setup_logger()

    fetcher = CountriesFetcher(token=args.token, logger=logger)
    writer = CSVWriter(output_path=args.output, logger=logger)

    try:
        countries = fetcher.get_countries()

        if countries:
            writer.save_checkpoint(countries)
            writer.write(countries)
        else:
            logger.warning("No countries retrieved.")

    except Exception as e:
        logger.error(f"Execution failed: {e}")
    finally:
        writer.cleanup()

if __name__ == "__main__":
    main()
