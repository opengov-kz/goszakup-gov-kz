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
        logging.FileHandler('subject_all.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoszakupParser:
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
        
    def fetch_data(self) -> list:
        """Fetch data from API with pagination"""
        all_data = []
        search_after = None
        page_count = 0
        
        while True:
            try:
                if search_after:
                    self.params.update({"page": "next", "search_after": search_after})

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

                all_data.extend(items)
                page_count += 1
                logger.info(f"Fetched {len(all_data)} records (page {page_count})")

                search_after = items[-1]["pid"]

                # Save temporary checkpoint every 5000 records
                if len(all_data) >= 5000:
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
            pd.DataFrame(data).to_parquet(
                temp_file,
                index=False,
                engine="pyarrow"
            )
            logger.info(f"Created checkpoint: {temp_file}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def save_data(self, data: list):
        """Save final data to parquet file"""
        try:
            if data:
                pd.DataFrame(data).to_parquet(
                    self.output_file,
                    index=False,
                    engine="pyarrow"
                )
                logger.info(f"Data saved to {self.output_file}")
            else:
                logger.warning("No data to save")
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
            logger.info("Starting data collection...")
            data = self.fetch_data()
            self.save_data(data)
            logger.info(f"Collected {len(data)} records")
        except Exception as e:
            logger.error(f"Error during execution: {e}")
            raise
        finally:
            self.cleanup()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Goszakup API Parser")
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
    
    parser = GoszakupParser(
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