
import argparse
import logging
import os
import pandas as pd
import requests
import time
from pathlib import Path
import tempfile

def setup_logger(log_filename: str = "amendm_agreem_justif.log") -> logging.Logger:
    logger = logging.getLogger("AmendmAgreemJustifFetcher")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

class AmendmAgreemJustifFetcher:
    BASE_URL = "https://ows.goszakup.gov.kz/v3/refs/ref_amendm_agreem_justif"

    def __init__(self, token: str, logger: logging.Logger):
        self.token = token
        self.logger = logger
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.params = {"limit": 50}

    def get_justifications(self) -> list:
        all_justifications = []
        next_page_url = self.BASE_URL
        while next_page_url:
            self.logger.info(f"Fetching page: {next_page_url}")
            try:
                response = requests.get(next_page_url, headers=self.headers, params=self.params, timeout=20)
                response.raise_for_status()
                payload = response.json()
                items = payload.get("items", [])
                all_justifications.extend([
                    {
                        "id": item.get("id"),
                        "name_ru": item.get("name_ru"),
                        "name_kz": item.get("name_kz"),
                        "cname_ru": item.get("cname_ru"),
                        "cname_kz": item.get("cname_kz")
                    }
                    for item in items
                ])
                next_page_path = payload.get("next_page")
                if next_page_path:
                    next_page_url = f"https://ows.goszakup.gov.kz{next_page_path}"
                else:
                    next_page_url = None
            except requests.RequestException as e:
                self.logger.error(f"API request failed: {e}")
                raise
        self.logger.info(f"Total justifications fetched: {len(all_justifications)}")
        return all_justifications

class CSVWriter:
    def __init__(self, output_path: str, logger: logging.Logger):
        self.output_path = output_path
        self.logger = logger
        self.temp_dir = tempfile.mkdtemp()

    def write(self, data: list):
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
    parser = argparse.ArgumentParser(description="Download amendment agreement justifications from Goszakup API and save to CSV.")
    parser.add_argument("--token", required=True, help="Access token for Goszakup API")
    parser.add_argument("--output", default="amendm_agreem_justif.csv", help="Path to output CSV file (default: amendm_agreem_justif.csv)")
    return parser.parse_args()

def main():
    args = parse_args()
    logger = setup_logger()
    fetcher = AmendmAgreemJustifFetcher(token=args.token, logger=logger)
    writer = CSVWriter(output_path=args.output, logger=logger)

    try:
        justifications = fetcher.get_justifications()
        if justifications:
            writer.save_checkpoint(justifications)
            writer.write(justifications)
        else:
            logger.warning("No justifications retrieved.")
    except Exception as e:
        logger.error(f"Execution failed: {e}")
    finally:
        writer.cleanup()

if __name__ == "__main__":
    main()
