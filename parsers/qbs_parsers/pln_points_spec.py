# pln_points_spec.py

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
        logging.FileHandler('pln_points_spec.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoszakupPlansSpecParser:
    def __init__(self, token: str, year: int, output_file: str = None):
        self.token = token
        self.year = year
        self.output_file = output_file or f"PlnPointsSpec_{year}.parquet"
        self.graphql_url = "https://ows.goszakup.gov.kz/v3/graphql"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
        self.temp_dir = os.path.join("plans_data_spec", "temp")
        self.output_dir = "plans_data_spec"
        self.query_template = """
        query GetPlans($filter: PlansFiltersInput, $after: Int) {
            Plans(filter: $filter, limit: 200, after: $after) {
                id
                dateCreate
                PlansSpec {
                    id
                    plnPointsId
                    refEkrbId
                    ekrbCode
                    ekrbNameRu
                    ekrbNameKz
                    count
                    price
                    refFkrbSubprogramId
                    fkrbSubprogramCode
                    fkrbSubprogramNameRu
                    fkrbSubprogramNameKz
                    refFkrbId
                    abpCode
                    abpNameRu
                    abpNameKz
                    amount
                    refFkrbProgramId
                    fkrbProgramCode
                    fkrbProgramNameRu
                    fkrbProgramNameKz
                    isActive
                    isDeleted
                    systemId
                    indexDate
                }
            }
        }
        """
        self.start_date = datetime(year, 1, 1)
        self.end_date = datetime(year, 12, 31, 23, 59, 59)
        self.time_step = timedelta(days=7)
        self.batch_size_limit = 10000
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)

    def check_server_availability(self, timeout=10) -> bool:
        """Check if the server is available"""
        try:
            response = requests.get(self.graphql_url, headers=self.headers, timeout=timeout)
            return response.status_code == 200
        except:
            return False

    def fetch_data(self) -> tuple[list, int]:
        """Fetch data from GraphQL API for the specified year with pagination"""
        plans_batch = []
        total_loaded = 0
        plans_without_spec = 0
        request_count = 0
        batch_count = 0
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

            range_completed = False
            retry_range_attempts = 0
            max_range_retries = 5

            while not range_completed and retry_range_attempts < max_range_retries:
                retry_range_attempts += 1
                logger.info(f"Processing range {current_start_date} - {current_end_date} (attempt {retry_range_attempts}/{max_range_retries})")

                while True:
                    request_count += 1
                    logger.info(f"Request #{request_count} for {current_start_date} - {current_end_date}")

                    max_retries = 3
                    response = None
                    for attempt in range(max_retries):
                        try:
                            response = requests.post(
                                self.graphql_url,
                                json={"query": self.query_template, "variables": variables},
                                headers=self.headers,
                                timeout=15
                            )
                            response.raise_for_status()
                            break
                        except Exception as e:
                            logger.warning(f"Error (attempt {attempt + 1}/{max_retries}): {e}")
                            if attempt < max_retries - 1:
                                time.sleep(5)
                            else:
                                if not self.check_server_availability():
                                    logger.warning("Server unavailable. Waiting 30 seconds...")
                                    time.sleep(30)
                                else:
                                    logger.error("Server available but request failed.")
                                break

                    if response is None:
                        logger.error("Request failed. Retrying range.")
                        break

                    data = response.json()
                    plans = data.get("data", {}).get("Plans", [])
                    if not plans:
                        logger.info("No data for this interval.")
                        range_completed = True
                        break

                    count_loaded = 0
                    for plan in plans:
                        plans_spec = plan.get("PlansSpec", [])
                        if not plans_spec:
                            plans_without_spec += 1
                            continue

                        for spec in plans_spec:
                            plans_batch.append({
                                "plan_id": plan.get("id"),
                                "dateCreate": plan.get("dateCreate"),
                                "specId": spec.get("id"),
                                "plnPointsId": spec.get("plnPointsId"),
                                "refEkrbId": spec.get("refEkrbId"),
                                "ekrbCode": spec.get("ekrbCode"),
                                "ekrbNameRu": spec.get("ekrbNameRu"),
                                "ekrbNameKz": spec.get("ekrbNameKz"),
                                "count": spec.get("count"),
                                "price": spec.get("price"),
                                "refFkrbSubprogramId": spec.get("refFkrbSubprogramId"),
                                "fkrbSubprogramCode": spec.get("fkrbSubprogramCode"),
                                "fkrbSubprogramNameRu": spec.get("fkrbSubprogramNameRu"),
                                "fkrbSubprogramNameKz": spec.get("fkrbSubprogramNameKz"),
                                "refFkrbId": spec.get("refFkrbId"),
                                "abpCode": spec.get("abpCode"),
                                "abpNameRu": spec.get("abpNameRu"),
                                "abpNameKz": spec.get("abpNameKz"),
                                "amount": spec.get("amount"),
                                "refFkrbProgramId": spec.get("refFkrbProgramId"),
                                "fkrbProgramCode": spec.get("fkrbProgramCode"),
                                "fkrbProgramNameRu": spec.get("fkrbProgramNameRu"),
                                "fkrbProgramNameKz": spec.get("fkrbProgramNameKz"),
                                "isActive": spec.get("isActive"),
                                "isDeleted": spec.get("isDeleted"),
                                "systemId": spec.get("systemId"),
                                "indexDate": spec.get("indexDate")
                            })
                            count_loaded += 1

                    total_loaded += count_loaded
                    logger.info(f"Loaded {count_loaded} specifications. Total: {total_loaded}")

                    if len(plans_batch) >= self.batch_size_limit:
                        batch_count += 1
                        self._save_checkpoint(plans_batch, batch_count)
                        plans_batch = []

                    last_id = plans[-1].get("id")
                    if last_id and len(plans) == 200:
                        variables["after"] = int(last_id) if isinstance(last_id, str) and last_id.isdigit() else last_id
                    else:
                        range_completed = True
                        break

                    time.sleep(1)

                if not range_completed:
                    logger.warning(f"Range not completed, retrying in 30 seconds...")
                    time.sleep(30)

            if not range_completed:
                logger.error(f"Failed to process range {current_start_date} - {current_end_date} after {max_range_retries} attempts. Skipping.")

            current_end_date = current_start_date - timedelta(seconds=1)

        if plans_batch:
            batch_count += 1
            self._save_checkpoint(plans_batch, batch_count)

        return batch_count, plans_without_spec

    def _save_checkpoint(self, data: list, batch_count: int):
        """Save temporary checkpoint file"""
        temp_file = os.path.join(self.temp_dir, f"plans_batch_{batch_count}.parquet")
        try:
            df = pd.DataFrame(data)
            df.to_parquet(temp_file, index=False)
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
                if os.path.exists(self.output_file):
                    existing_df = pd.read_parquet(self.output_file)
                    all_data = pd.concat([existing_df, all_data]).drop_duplicates(
                        subset=["plan_id", "specId"], keep="last"
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
            logger.info(f"Starting PlansSpec data collection for year {self.year}...")
            batch_count, plans_without_spec = self.fetch_data()
            self.combine_and_save(batch_count)
            final_count = len(pd.read_parquet(self.output_file)) if os.path.exists(self.output_file) else 0
            logger.info(f"Completed. Total records: {final_count}, Plans without PlansSpec: {plans_without_spec}")
        except Exception as e:
            logger.error(f"Error during execution: {e}")
            raise
        finally:
            self.cleanup()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Goszakup PlansSpec GraphQL API Parser")
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
        help="Output parquet file name (default: PlnPointsSpec_<year>.parquet)"
    )
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    
    parser = GoszakupPlansSpecParser(
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