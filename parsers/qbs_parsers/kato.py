
import argparse
import logging
import os
import pandas as pd
import requests
import time
from pathlib import Path
import tempfile

def setup_logger(log_filename: str = "kato.log") -> logging.Logger:
    """Configure and return a logger."""
    logger = logging.getLogger("KatoParser")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

class KatoFetcher:
    """Class to interact with the Goszakup API and retrieve KATO codes."""

    BASE_URL = "https://ows.goszakup.gov.kz/v3/refs/ref_kato"

    def __init__(self, token: str, logger: logging.Logger):
        self.token = token
        self.logger = logger
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.params = {"limit": 100}

    def get_kato_list(self) -> list:
        """Fetch KATO codes from the API."""
        self.logger.info("Sending request to fetch KATO list...")
        all_kato = []
        search_after = None

        while True:
            try:
                if search_after:
                    self.params.update({"page": "next", "search_after": search_after})

                response = requests.get(
                    self.BASE_URL,
                    headers=self.headers,
                    params=self.params,
                    timeout=30
                )
                response.raise_for_status()
                payload = response.json()

                items = payload.get("items", [])
                if not items:
                    self.logger.info("No more KATO data to fetch.")
                    break

                for item in items:
                    all_kato.append({
                        "code": item.get("code"),
                        "name_kz": item.get("name_kz"),
                        "name_ru": item.get("name_ru"),
                        "full_name_kz": item.get("full_name_kz"),
                        "full_name_ru": item.get("full_name_ru"),
                        "parent_code": item.get("parent_code"),
                        "level_": item.get("level_"),
                        "hij": item.get("hij"),
                        "k": item.get("k"),
                        "ef": item.get("ef"),
                        "ab": item.get("ab"),
                        "cd": item.get("cd")
                    })

                search_after = items[-1]["code"]
                if not payload.get("next_page"):
                    break

                time.sleep(2)

            except requests.RequestException as e:
                self.logger.error(f"API request failed: {e}")
                raise

        self.logger.info(f"Successfully fetched {len(all_kato)} KATO records.")
        return all_kato

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
    parser = argparse.ArgumentParser(description="Download KATO list from Goszakup API and save to CSV.")
    parser.add_argument("--token", required=True, help="Access token for Goszakup API")
    parser.add_argument("--output", default="kato.csv", help="Path to output CSV file (default: kato.csv)")
    return parser.parse_args()

def main():
    args = parse_args()
    logger = setup_logger()

    fetcher = KatoFetcher(token=args.token, logger=logger)
    writer = CSVWriter(output_path=args.output, logger=logger)

    try:
        kato_list = fetcher.get_kato_list()

        if kato_list:
            writer.save_checkpoint(kato_list)
            writer.write(kato_list)
        else:
            logger.warning("No KATO records retrieved.")

    except Exception as e:
        logger.error(f"Execution failed: {e}")
    finally:
        writer.cleanup()

if __name__ == "__main__":
    main()
