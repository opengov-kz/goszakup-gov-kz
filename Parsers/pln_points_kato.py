# pln_points_kato.py

import requests
import pandas as pd
import time
import os
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pln_points_kato.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoszakupPlansKatoParser:
    def __init__(self, token: str, year: int, output_file: str = None):
        self.token = token
        self.year = year
        self.output_file = output_file or f"PlnPointsKato_{year}.parquet"
        self.graphql_url = "https://ows.goszakup.gov.kz/v3/graphql"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
        self.temp_dir = os.path.join("plans_data", "temp")
        self.output_dir = "plans_data"
        self.query_template = """
        query GetPlans($filter: PlansFiltersInput, $after: Int) {
            Plans(filter: $filter, limit: 200, after: $after) {
                id
                dateCreate
                PlansKato {
                    id
                    plnPointsId
                    refKatoCode
                    refCountriesCode
                    fullDeliveryPlaceNameRu
                    fullDeliveryPlaceNameKz
                    count
                    systemId
                }
            }
        }
        """
        self.start_date = datetime(year, 1, 1)
        self.end_date = datetime(year, 12, 31, 23, 59, 59)
        self.time_step = timedelta(days=7)
        self.batch_size_limit = 100000
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)

    def load_existing_data(self) -> pd.DataFrame:
        """Load existing data if it exists"""
        try:
            if os.path.exists(self.output_file):
                existing_df = pd.read_parquet(self.output_file).dropna(subset=["dateCreate"])
                logger.info(f"Loaded {len(existing_df)} existing records")
                return existing_df
            logger.info("No existing data found. Starting fresh.")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to load existing data: {e}")
            raise

    def fetch_data(self) -> list:
        """Fetch data from GraphQL API for the specified year with pagination"""
        plans_batch = []
        total_loaded = 0
        request_count = 0
        batch_count = 0
        error_attempts = 0
        current_end_date = self.end_date

        while current_end_date >= self.start_date:
            current_start_date = max(current_end_date - self.time_step, self.start_date)
            variables = {
                "filter": {
                    "dateCreate": {
                        "gte": current_start_date.strftime("%Y-%m-%d %H:%M:%S"),
                        "lte": current_end_date.strftime("%Y-%m-%d %H:%M:%S")
                    }
                },
                "after": None
            }

            while True:  # Pagination loop within time interval
                request_count += 1
                logger.info(f"Request #{request_count} (filter={variables['filter']}, after={variables['after']})")

                try:
                    response = requests.post(
                        self.graphql_url,
                        json={"query": self.query_template, "variables": variables},
                        headers=self.headers,
                        timeout=15
                    )
                    response.raise_for_status()
                    data = response.json()

                    if "errors" in data:
                        logger.error(f"API error: {data['errors']}")
                        error_attempts += 1
                        if error_attempts >= 5:
                            logger.error("Reached error limit. Stopping.")
                            break
                        time.sleep(5)
                        continue

                    plans = data.get("data", {}).get("Plans")
                    if plans is None:
                        logger.warning("Plans returned None. Moving to next interval.")
                        break
                    if not plans:
                        logger.info("No data for this interval or page.")
                        break

                    count_loaded = 0
                    for plan in plans:
                        kato_list = plan.get("PlansKato")
                        if kato_list is None:
                            continue
                        for kato in kato_list:
                            plans_batch.append({
                                "plan_id": plan["id"],
                                "dateCreate": plan["dateCreate"],
                                "kato_id": kato["id"],
                                "plnPointsId": kato["plnPointsId"],
                                "refKatoCode": kato["refKatoCode"],
                                "refCountriesCode": kato["refCountriesCode"],
                                "fullDeliveryPlaceNameRu": kato["fullDeliveryPlaceNameRu"],
                                "fullDeliveryPlaceNameKz": kato["fullDeliveryPlaceNameKz"],
                                "count": kato["count"],
                                "systemId": kato["systemId"]
                            })
                            count_loaded += 1

                    total_loaded += count_loaded
                    logger.info(f"Loaded {count_loaded} records. Total loaded: {total_loaded}")
                    if plans:
                        logger.info(f"Last date in response: {plans[-1]['dateCreate']}")

                    if len(plans_batch) >= self.batch_size_limit:
                        batch_count += 1
                        self._save_checkpoint(plans_batch, batch_count)
                        plans_batch = []

                    # Check pagination
                    page_info = data.get("extensions", {}).get("pageInfo", {})
                    if page_info.get("hasNextPage", False):
                        variables["after"] = page_info.get("lastId")
                    else:
                        break  # No next page

                    error_attempts = 0
                    time.sleep(1)

                except requests.exceptions.Timeout:
                    logger.warning("Request timeout. Retrying in 5 seconds...")
                    error_attempts += 1
                    if error_attempts >= 5:
                        logger.error("Reached error limit. Stopping.")
                        break
                    time.sleep(5)
                except Exception as e:
                    logger.error(f"Error: {e}")
                    error_attempts += 1
                    if error_attempts >= 5:
                        logger.error("Reached error limit. Stopping.")
                        break
                    time.sleep(5)

            current_end_date = current_start_date - timedelta(seconds=1)
            error_attempts = 0

        if plans_batch:
            batch_count += 1
            self._save_checkpoint(plans_batch, batch_count)

        return batch_count

    def _save_checkpoint(self, data: list, batch_count: int):
        """Save temporary checkpoint file"""
        temp_file = os.path.join(self.temp_dir, f"plans_batch_{batch_count}.parquet")
        try:
            pd.DataFrame(data).to_parquet(temp_file, index=False)
            logger.info(f"Saved checkpoint: {temp_file} ({len(data)} records)")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def combine_and_save(self, batch_count: int):
        """Combine temporary files and save final data"""
        try:
            temp_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.endswith(".parquet")]
            if temp_files:
                logger.info(f"Combining {len(temp_files)} temporary files...")
                df_list = [pd.read_parquet(f) for f in temp_files]
                all_data = pd.concat(df_list, ignore_index=True)
                existing_df = self.load_existing_data()
                if not existing_df.empty:
                    all_data = pd.concat([existing_df, all_data]).drop_duplicates(
                        subset=["plan_id", "kato_id"], keep="last"
                    )
                all_data.to_parquet(self.output_file, index=False)
                logger.info(f"Data saved to {self.output_file} ({len(all_data)} records)")
                for temp_file in temp_files:
                    os.remove(temp_file)
                logger.info("Temporary files deleted.")
            else:
                logger.warning("No new data to save.")
        except Exception as e:
            logger.error(f"Failed to combine and save data: {e}")
            raise

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

    def run(self):
        """Main execution method"""
        try:
            logger.info(f"Starting PlansKato data collection for year {self.year}...")
            batch_count = self.fetch_data()
            self.combine_and_save(batch_count)
            final_count = len(pd.read_parquet(self.output_file)) if os.path.exists(self.output_file) else 0
            logger.info(f"Completed. Total records in final file: {final_count}")
        except Exception as e:
            logger.error(f"Error during execution: {e}")
            raise
        finally:
            self.cleanup()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Goszakup PlansKato GraphQL API Parser")
    parser.add_argument(
        "--token",
        required=True,
        help="API authentication token"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year to fetch data for (e.g., 2023)"
    )
    parser.add_argument(
        "--output",
        help="Output parquet file name (default: PlnPointsKato_<year>.parquet)"
    )
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    
    parser = GoszakupPlansKatoParser(
        token=args.token,
        year=args.year,
        output_file=args.output
    )
    
    try:
        parser.run()
    except Exception as e:
        logger.error(f"Program terminated with error: {e}")
        raise

if __name__ == "__main__":
    main()