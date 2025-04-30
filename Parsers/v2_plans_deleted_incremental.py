# v2_plans_deleted_incremental.py

import requests
import time
import pandas as pd
import os
import logging
from datetime import datetime, timedelta
import argparse
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('v2_plans_deleted_incremental.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoszakupDeletedPlansIncrementalParser:
    def __init__(self, token: str, output_file: str = "v2PlansDeleted.parquet"):
        self.token = token
        self.output_file = output_file
        self.base_url = "https://ows.goszakup.gov.kz"
        self.deleted_endpoint = "/v3/plans/deleted"
        self.view_endpoint = "/v3/plans/view"
        self.temp_dir = "temp_parquet_files"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.params = {"limit": 500}
        self.start_date = datetime(2024, 1, 1)
        self.stop_date = datetime.now()
        self.batch_size = 10000
        self.max_no_new_pages = 3
        self.date_create_cache = {}  # Cache for rootrecord_id -> date_create
        os.makedirs(self.temp_dir, exist_ok=True)

    def parse_date(self, date_str: str) -> datetime:
        """Parse date string in supported formats, return None if invalid"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                logger.warning(f"Unsupported date format: {date_str}")
                return None

    def get_date_create(self, rootrecord_id: str) -> datetime:
        """Fetch date_create from /v3/plans/view/{rootrecord_id}"""
        if not rootrecord_id:
            logger.warning("Missing rootrecord_id")
            return None

        if rootrecord_id in self.date_create_cache:
            return self.date_create_cache[rootrecord_id]

        try:
            url = f"{self.base_url}{self.view_endpoint}/{rootrecord_id}"
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            date_create_raw = data.get("date_create")
            date_create = self.parse_date(date_create_raw) if date_create_raw else None
            self.date_create_cache[rootrecord_id] = date_create
            if date_create is None and date_create_raw:
                logger.warning(f"Invalid date_create for rootrecord_id {rootrecord_id}: {date_create_raw}")
            return date_create
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch date_create for rootrecord_id {rootrecord_id}: {e}")
            self.date_create_cache[rootrecord_id] = None
            return None

    def load_existing_max_date(self) -> datetime:
        """Load the maximum date_create from the existing parquet file"""
        try:
            if os.path.exists(self.output_file):
                df_existing = pd.read_parquet(self.output_file)
                if not df_existing.empty and 'date_create' in df_existing.columns:
                    max_date_create = pd.to_datetime(df_existing["date_create"]).max()
                    if pd.isna(max_date_create):
                        logger.info("No valid date_create in existing file, starting from scratch")
                        return None
                    logger.info(f"Found {len(df_existing)} existing records, max date_create: {max_date_create}")
                    return max_date_create
                else:
                    logger.info("Existing file is empty or missing date_create column")
            else:
                logger.info("No existing parquet file found")
            return None
        except Exception as e:
            logger.error(f"Failed to load existing parquet file: {e}")
            return None

    def save_batch(self, data: list, batch_count: int) -> int:
        """Save batch to temporary parquet file"""
        temp_file = os.path.join(self.temp_dir, f"temp_batch_{batch_count}.parquet")
        try:
            df = pd.DataFrame(data)
            df.to_parquet(temp_file, index=False, engine="pyarrow")
            logger.info(f"Saved temporary file: {temp_file} with {len(data)} records")
            return batch_count + 1
        except Exception as e:
            logger.error(f"Failed to save batch: {e}")
            raise

    def merge_temp_files(self, new_data: list):
        """Merge temporary files with existing data and save to final output"""
        try:
            temp_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.endswith(".parquet")]
            if not temp_files and not new_data:
                logger.info("No new data to merge.")
                return

            combined_df = pd.DataFrame(new_data) if new_data else pd.DataFrame()
            if temp_files:
                temp_dfs = [pd.read_parquet(f) for f in temp_files]
                combined_df = pd.concat([combined_df] + temp_dfs, ignore_index=True)

            if os.path.exists(self.output_file):
                existing_df = pd.read_parquet(self.output_file)
                combined_df = pd.concat([existing_df, combined_df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=["id"], keep="last")

            if not combined_df.empty:
                combined_df.to_parquet(self.output_file, index=False, engine="pyarrow")
                logger.info(f"Merged data into {self.output_file}, total records: {len(combined_df)}")
            else:
                logger.info("No data to save after merging.")

            for f in temp_files:
                os.remove(f)
            if os.path.exists(self.temp_dir) and not os.listdir(self.temp_dir):
                os.rmdir(self.temp_dir)
        except Exception as e:
            logger.error(f"Failed to merge temporary files: {e}")
            raise

    def fetch_data(self) -> tuple[int, int]:
        """Fetch new deleted plans data incrementally"""
        max_date_create = self.load_existing_max_date()
        next_page = f"{self.deleted_endpoint}?limit=500"
        seen_ids = set()
        filtered_items = []
        total_records = 0
        no_new_data_count = 0
        temp_file_count = 0
        invalid_date_count = 0

        while next_page:
            try:
                response = requests.get(f"{self.base_url}{next_page}", headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()

                items = data.get("items", [])
                if not items:
                    logger.info("No more data to fetch.")
                    break

                logger.info(f"Received {len(items)} records on this page.")
                page_filtered_items = []
                page_has_new_data = False

                first_index_date = items[0].get("index_date", "None")
                last_index_date = items[-1].get("index_date", "None")
                logger.info(f"First index_date: {first_index_date}, Last index_date: {last_index_date}")

                for item in items:
                    if item["id"] in seen_ids:
                        logger.debug(f"Skipped duplicate record ID {item['id']}")
                        continue

                    index_date_raw = item.get("index_date")
                    item_index_date = None
                    if index_date_raw:
                        item_index_date = self.parse_date(index_date_raw)
                        if item_index_date is None:
                            logger.warning(f"Invalid index_date for record ID {item['id']}: {index_date_raw}")
                            invalid_date_count += 1

                    # Fetch date_create using rootrecord_id
                    rootrecord_id = item.get("rootrecord_id")
                    date_create = self.get_date_create(rootrecord_id)

                    # Include record if:
                    # - date_create is None/invalid (to avoid skipping)
                    # - date_create is within [start_date, stop_date]
                    # - date_create is newer than max_date_create (if exists)
                    if date_create is None or (self.start_date <= date_create <= self.stop_date):
                        if max_date_create is None or date_create is None or date_create > max_date_create:
                            item_copy = item.copy()
                            item_copy["index_date"] = item_index_date  # Store parsed index_date or None
                            item_copy["date_create"] = date_create  # Store parsed date_create or None
                            page_filtered_items.append(item_copy)
                            filtered_items.append(item_copy)
                            seen_ids.add(item["id"])
                            page_has_new_data = True
                            if date_create is None:
                                logger.debug(f"Included record ID {item['id']} with missing/invalid date_create")
                            else:
                                logger.debug(f"Included record ID {item['id']} with date_create {date_create}")
                        else:
                            logger.debug(f"Skipped record ID {item['id']} with date_create {date_create} (not newer than max_date_create {max_date_create})")
                    else:
                        logger.debug(f"Skipped record ID {item['id']} with date_create {date_create} (outside date range {self.start_date} to {self.stop_date})")

                    # Delay to avoid rate limits
                    time.sleep(0.5)

                logger.info(f"Total records on page: {len(items)}")
                logger.info(f"New records (based on date_create): {len(page_filtered_items)}")
                if page_filtered_items:
                    for i, item in enumerate(page_filtered_items[:3]):
                        logger.info(f"Sample record {i+1} - ID: {item['id']}, index_date: {item.get('index_date', 'None')}, date_create: {item.get('date_create', 'None')}")

                if page_has_new_data:
                    no_new_data_count = 0
                    total_records += len(page_filtered_items)
                    logger.info(f"Added {len(page_filtered_items)} new records from this page. Total: {total_records}")
                else:
                    no_new_data_count += 1
                    logger.info(f"No new records on this page ({no_new_data_count}/{self.max_no_new_pages}).")

                if len(filtered_items) >= self.batch_size:
                    temp_file_count = self.save_batch(filtered_items, temp_file_count)
                    filtered_items = []

                if no_new_data_count >= self.max_no_new_pages:
                    logger.info(f"Reached limit of {self.max_no_new_pages} pages with no new data, stopping.")
                    break

                next_page = data.get("next_page")
                if not next_page:
                    logger.info("Reached last page, stopping.")
                    break

                time.sleep(5)

            except requests.exceptions.Timeout:
                logger.error("Request timeout, waiting 30 seconds before retry...")
                time.sleep(30)
                continue
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error: {e}, waiting 30 seconds before retry...")
                time.sleep(30)
                continue
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}, waiting 30 seconds before retry...")
                time.sleep(30)
                continue

        if filtered_items:
            temp_file_count = self.save_batch(filtered_items, temp_file_count)

        logger.info(f"Records with invalid or missing index_date: {invalid_date_count}")
        return total_records, invalid_date_count

    def run(self):
        """Main execution method"""
        try:
            logger.info("Starting incremental deleted plans data collection...")
            total_records, invalid_date_count = self.fetch_data()
            self.merge_temp_files([])
            if total_records == 0:
                logger.info("No new data found.")
            else:
                logger.info(f"Completed. Total new records fetched: {total_records}, including {invalid_date_count} with invalid/missing index_date")
        except Exception as e:
            logger.error(f"Error during execution: {e}")
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up temporary files if any remain"""
        try:
            for temp_file in Path(self.temp_dir).glob("*.parquet"):
                temp_file.unlink()
            if os.path.exists(self.temp_dir) and not os.listdir(self.temp_dir):
                os.rmdir(self.temp_dir)
            logger.info("Temporary files cleaned up.")
        except Exception as e:
            logger.error(f"Failed to clean up temporary files: {e}")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Goszakup Deleted Plans Incremental API Parser")
    parser.add_argument(
        "--token",
        required=True,
        help="API authentication token"
    )
    parser.add_argument(
        "--output",
        default="v2PlansDeleted.parquet",
        help="Output parquet file name (default: v2PlansDeleted.parquet)"
    )
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    
    parser = GoszakupDeletedPlansIncrementalParser(
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