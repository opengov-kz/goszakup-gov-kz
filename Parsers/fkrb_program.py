
import argparse
import logging
import os
import pandas as pd
import requests
import time
from pathlib import Path
import tempfile

def setup_logger(log_filename: str = "fkrb_program.log") -> logging.Logger:
    logger = logging.getLogger("FkrbProgramFetcher")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

class FkrbProgramFetcher:
    BASE_URL = "https://ows.goszakup.gov.kz/v3/refs/ref_fkrb_program"

    def __init__(self, token: str, logger: logging.Logger):
        self.token = token
        self.logger = logger
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.params = {"limit": 100}

    def get_programs(self) -> list:
        self.logger.info("Fetching Fkrb Programs...")
        try:
            response = requests.get(
                self.BASE_URL,
                headers=self.headers,
                params=self.params,
                timeout=20
            )
            response.raise_for_status()
            payload = response.json()

            programs = [
                {
                    "id": item.get("id"),
                    "code": item.get("code"),
                    "prg": item.get("prg"),
                    "name_kz": item.get("name_kz"),
                    "name_ru": item.get("name_ru")
                }
                for item in payload.get("items", [])
            ]

            self.logger.info(f"Fetched {len(programs)} programs.")
            return programs

        except requests.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            raise

class CSVWriter:
    def __init__(self, output_path: str, logger: logging.Logger):
        self.output_path = output_path
        self.logger = logger
        self.temp_dir = tempfile.mkdtemp()

    def write(self, data: list):
        if not data:
            self.logger.warning("No data to write to CSV.")
            return

        df = pd.DataFrame(data)
        try:
            df.to_csv(
                self.output_path,
                index=False,
                encoding="utf-8-sig",
                sep="|",
                quotechar="'",
                escapechar="\"
            )
            self.logger.info(f"Data written to {self.output_path}")
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
                escapechar="\"
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
    parser = argparse.ArgumentParser(description="Download Fkrb Programs from Goszakup API and save to CSV.")
    parser.add_argument("--token", required=True, help="Access token for Goszakup API")
    parser.add_argument("--output", default="fkrb_programs.csv", help="Path to output CSV file")
    return parser.parse_args()

def main():
    args = parse_args()
    logger = setup_logger()

    fetcher = FkrbProgramFetcher(token=args.token, logger=logger)
    writer = CSVWriter(output_path=args.output, logger=logger)

    try:
        programs = fetcher.get_programs()

        if programs:
            writer.save_checkpoint(programs)
            writer.write(programs)
        else:
            logger.warning("No programs retrieved.")

    except Exception as e:
        logger.error(f"Execution failed: {e}")
    finally:
        writer.cleanup()

if __name__ == "__main__":
    main()
