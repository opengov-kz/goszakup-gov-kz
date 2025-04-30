import requests
import time
import pandas as pd
import os
import argparse
import logging
import tempfile
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('subject_all_incremental.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoszakupIncrementalParser:
    def __init__(self, token: str, output_file: str = "v3SubjectAll.parquet"):
        self.token = token
        self.output_file = output_file
        self.base_url = "https://ows.goszakup.gov.kz/v3/subject/all"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.params = {"limit": 500}
        self.temp_dir = tempfile.mkdtemp()
        self.existing_data = None
        self.last_update_date = None

    def load_existing_data(self):
        """Load existing data from parquet file if it exists"""
        try:
            if os.path.exists(self.output_file):
                self.existing_data = pd.read_parquet(self.output_file, engine="pyarrow")
                self.last_update_date = self.existing_data["last_update_date"].max()
                logger.info(f"Existing data loaded. Last update date: {self.last_update_date}")
            else:
                logger.info("No existing data found. Starting fresh.")
        except Exception as e:
            logger.error(f"Failed to load existing data: {e}")
            raise

    def fetch_data(self) -> list:
        """Fetch new data from API based on last_update_date"""
        all_data = []
        page_count = 0

        while True:
            try:
                if self.last_update_date:
                    self.params.update({"last_update_date": self.last_update_date})

                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=self.params,
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()

                items = data.get("items", [])
                if not items:
                    logger.info("No more data to fetch")
                    break

                # Filter items newer than last_update_date
                new_items = [
                    item for item in items
                    if not self.last_update_date or item["last_update_date"] > self.last_update_date
                ]
                if not new_items:
                    logger.info("No new records found")
                    break

                all_data.extend(new_items)
                page_count += 1
                logger.info(f"Fetched {len(all_data)} new records (page {page_count})")

                self.last_update_date = max(item["last_update_date"] for item in new_items)

                # Save temporary checkpoint every 1000 records
                if len(all_data) >= 1000:
                    self._save_checkpoint(all_data)

                time.sleep(5)  # Respectful API rate limiting

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                logger.info("Waiting 30 seconds before retry...")
                time.sleep(30)
                continue

        return all_data

    def _save_checkpoint(self, data: list):
        """Save temporary checkpoint file"""
        temp_file = os.path.join(self.temp_dir, f"checkpoint_{int(time.time())}.parquet")
        try:
            df_new = pd.DataFrame(data)
            if self.existing_data is not None:
                df_combined = pd.concat([self.existing_data, df_new], ignore_index=True)
            else:
                df_combined = df_new
            df_combined.to_parquet(temp_file, index=False, engine="pyarrow")
            logger.info(f"Created checkpoint: {temp_file}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def save_data(self, data: list):
        """Save final data to parquet file"""
        try:
            if data:
                df_new = pd.DataFrame(data)
                if self.existing_data is not None:
                    df_combined = pd.concat([self.existing_data, df_new], ignore_index=True)
                else:
                    df_combined = df_new
                df_combined.to_parquet(self.output_file, index=False, engine="pyarrow")
                logger.info(f"Data saved to {self.output_file}")
            else:
                logger.warning("No new data to save")
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            raise

    def cleanup(self):
        """Clean up temporary files"""
        try:
            for temp_file in Path(self.temp_dir).glob("*.parquet"):
                temp_file.unlink()
            os.rmdir(self.temp_dir)
            logger.info("Temporary files cleaned up")
        except Exception as e:
            logger.error(f"Failed to clean up temporary files: {e}")

    def run(self):
        """Main execution method"""
        try:
            logger.info("Starting incremental data collection...")
            self.load_existing_data()
            data = self.fetch_data()
            self.save_data(data)
            logger.info(f"Collected {len(data)} new records")
        except Exception as e:
            logger.error(f"Error during execution: {e}")
            raise
        finally:
            self.cleanup()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Goszakup Incremental API Parser")
    parser.add_argument(
        "--token",
        required=True,
        help="API authentication token"
    )
    parser.add_argument(
        "--output",
        default="v3SubjectAll.parquet",
        help="Output parquet file name (default: v3SubjectAll.parquet)"
    )
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    
    parser = GoszakupIncrementalParser(
        token=args.token,
        output_file=args.output
    )
    
    try:
        parser.run()
    except Exception as e:
        logger.error(f"Program terminated with error: {e}")
        raise

if __name__ == "__main__":
    main()