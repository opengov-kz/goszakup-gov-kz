# contract_act_incremental.py

import requests
import pandas as pd
import time
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
        logging.FileHandler('contract_act_incremental.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://ows.goszakup.gov.kz/v3/graphql"

QUERY_TEMPLATE = """
query GetContractActs($filter: ContractActFiltersInput, $after: Int) {
    Acts(filter: $filter, limit: 200, after: $after) {
        id
        aktDate
        numberAct
        approveDate
        revokeDate
        createDateAct
        contractRootId
        contractId
        statusId
        isDeleted
        dayOverdue
        sumAvans
        sumBeginning
        sumFine
        sumPreviously
        sumTransfer
        createDateGenInfo
        statusNameRu
        statusNameKz
        supplierId
        customerId
        isGu
        typeAct
        refSubjectTypeId
        parentId
        systemId
        indexDate
    }
}
"""

class ContractActIncrementalParser:
    def __init__(self, token: str, output_prefix: str = "ContractAct", deduplicate: bool = True):
        self.token = token
        self.base_prefix = output_prefix
        self.deduplicate = deduplicate
        self.output_dir = "contract_act_data"
        self.temp_dir = os.path.join(self.output_dir, "temp")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Output file
        self.contract_act_file = os.path.join(self.output_dir, f"{self.base_prefix}.parquet")
        
        self.batch_size_limit = 100000
        self.time_step = timedelta(days=7)
        self.end_date = datetime.now()
        self.start_date = datetime(2024, 1, 1)  # Fallback start date
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }

    def load_existing_max_date(self) -> datetime:
        """Load the maximum date from approveDate or revokeDate in the existing parquet file and temp files"""
        try:
            max_date = None
            # Check main contract acts file
            if os.path.exists(self.contract_act_file):
                df_existing = pd.read_parquet(self.contract_act_file)
                logger.info(f"Loaded existing file with {len(df_existing)} records")
                if not df_existing.empty:
                    approve_dates = pd.to_datetime(df_existing["approveDate"], errors="coerce")
                    revoke_dates = pd.to_datetime(df_existing["revokeDate"], errors="coerce")
                    valid_dates = pd.concat([approve_dates, revoke_dates], axis=1).max(axis=1)
                    valid_dates = valid_dates.dropna()
                    if not valid_dates.empty:
                        max_date = valid_dates.max()
                        logger.info(f"Max date in {self.contract_act_file}: {max_date}, valid date count: {len(valid_dates)}")
                    else:
                        logger.warning("No valid approveDate or revokeDate values in existing file")

            # Check temporary files
            temp_contract_act_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.startswith("contract_act_batch_") and f.endswith(".parquet")]
            for temp_file in temp_contract_act_files:
                temp_df = pd.read_parquet(temp_file)
                if not temp_df.empty:
                    approve_dates = pd.to_datetime(temp_df["approveDate"], errors="coerce")
                    revoke_dates = pd.to_datetime(temp_df["revokeDate"], errors="coerce")
                    valid_dates = pd.concat([approve_dates, revoke_dates], axis=1).max(axis=1)
                    valid_dates = valid_dates.dropna()
                    if not valid_dates.empty:
                        temp_max_date = valid_dates.max()
                        if max_date is None or (temp_max_date is not None and temp_max_date > max_date):
                            max_date = temp_max_date
                            logger.info(f"Updated max date from temp file {temp_file}: {max_date}")

            if max_date is not None:
                return max_date + timedelta(seconds=1)
            else:
                logger.info(f"No valid dates found. Starting from {self.start_date}")
                return self.start_date
        except Exception as e:
            logger.error(f"Failed to load existing data: {e}")
            return self.start_date

    def fetch_data(self) -> int:
        """Fetch new ContractAct data incrementally"""
        current_start_date = self.load_existing_max_date()
        contract_act_batch = []
        batch_count = 0
        total_loaded = 0
        request_count = 0
        error_attempts = 0

        # Initialize batch_count based on existing temporary files
        existing_temp_files = [f for f in os.listdir(self.temp_dir) if f.startswith("contract_act_batch_") and f.endswith(".parquet")]
        for f in existing_temp_files:
            try:
                batch_number = int(f.replace("contract_act_batch_", "").replace(".parquet", ""))
                batch_count = max(batch_count, batch_number)
            except ValueError:
                continue

        while current_start_date < self.end_date:
            current_end_date = min(current_start_date + self.time_step, self.end_date)
            variables = {
                "filter": {
                    "dateAction": [
                        current_start_date.strftime("%Y-%m-%d %H:%M:%S"),
                        current_end_date.strftime("%Y-%m-%d %H:%M:%S")
                    ]
                },
                "after": None
            }
            
            logger.info(f"Fetching data for {current_start_date} to {current_end_date}")
            
            while True:  # Pagination loop
                request_count += 1
                logger.info(f"Request #{request_count} (filter={variables['filter']}, after={variables['after']})")

                try:
                    response = requests.post(GRAPHQL_URL, json={"query": QUERY_TEMPLATE, "variables": variables}, headers=self.headers, timeout=15)
                    response.raise_for_status()
                    data = response.json()

                    if "errors" in data:
                        logger.error(f"API error: {data['errors']}")
                        error_attempts += 1
                        if error_attempts >= 5:
                            logger.error("Reached error limit. Stopping.")
                            return total_loaded
                        time.sleep(5)
                        continue

                    contract_acts = data.get("data", {}).get("Acts")
                    if contract_acts is None:
                        logger.warning("Acts returned None. Moving to next interval.")
                        break
                    if not contract_acts:
                        logger.info("No data for this interval or page ended.")
                        break

                    count_loaded = 0
                    for act in contract_acts:
                        contract_act_batch.append({
                            "id": act["id"],
                            "aktDate": act["aktDate"],
                            "numberAct": act["numberAct"],
                            "approveDate": act["approveDate"],
                            "revokeDate": act["revokeDate"],
                            "createDateAct": act["createDateAct"],
                            "contractRootId": act["contractRootId"],
                            "contractId": act["contractId"],
                            "statusId": act["statusId"],
                            "isDeleted": act["isDeleted"],
                            "dayOverdue": act["dayOverdue"],
                            "sumAvans": act["sumAvans"],
                            "sumBeginning": act["sumBeginning"],
                            "sumFine": act["sumFine"],
                            "sumPreviously": act["sumPreviously"],
                            "sumTransfer": act["sumTransfer"],
                            "createDateGenInfo": act["createDateGenInfo"],
                            "statusNameRu": act["statusNameRu"],
                            "statusNameKz": act["statusNameKz"],
                            "supplierId": act["supplierId"],
                            "customerId": act["customerId"],
                            "isGu": act["isGu"],
                            "typeAct": act["typeAct"],
                            "refSubjectTypeId": act["refSubjectTypeId"],
                            "parentId": act["parentId"],
                            "systemId": act["systemId"],
                            "indexDate": act["indexDate"]
                        })
                        count_loaded += 1
                    
                    total_loaded += count_loaded
                    last_date = contract_acts[-1]["approveDate"] or contract_acts[-1]["revokeDate"]
                    logger.info(f"Loaded {count_loaded} records. Total loaded: {total_loaded}, Last date: {last_date}")

                    if len(contract_act_batch) >= self.batch_size_limit:
                        batch_count += 1
                        temp_contract_act_file = os.path.join(self.temp_dir, f"contract_act_batch_{batch_count}.parquet")
                        pd.DataFrame(contract_act_batch).to_parquet(temp_contract_act_file, index=False)
                        logger.info(f"Saved temporary file: {temp_contract_act_file} ({len(contract_act_batch)} records)")
                        contract_act_batch = []

                    page_info = data.get("extensions", {}).get("pageInfo", {})
                    if page_info.get("hasNextPage", False):
                        variables["after"] = page_info.get("lastId")
                    else:
                        break  # No next page

                    error_attempts = 0
                    time.sleep(1)

                except requests.exceptions.Timeout:
                    logger.warning("Request timeout. Retrying in 5 seconds...")
                    time.sleep(5)
                    error_attempts += 1
                    if error_attempts >= 5:
                        logger.error("Reached error limit. Stopping.")
                        return total_loaded
                except Exception as e:
                    logger.error(f"Error: {e}")
                    error_attempts += 1
                    if error_attempts >= 5:
                        logger.error("Reached error limit. Stopping.")
                        return total_loaded
                    time.sleep(5)

            current_start_date = current_end_date + timedelta(seconds=1)
            error_attempts = 0

        if contract_act_batch:
            batch_count += 1
            temp_contract_act_file = os.path.join(self.temp_dir, f"contract_act_batch_{batch_count}.parquet")
            pd.DataFrame(contract_act_batch).to_parquet(temp_contract_act_file, index=False)
            logger.info(f"Saved temporary file: {temp_contract_act_file} ({len(contract_act_batch)} records)")

        return total_loaded

    def merge_temp_files(self):
        """Merge temporary files into the final output"""
        try:
            temp_contract_act_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.startswith("contract_act_batch_") and f.endswith(".parquet")]
            if not temp_contract_act_files:
                logger.info(f"No new data to merge for {self.contract_act_file}. Existing file unchanged.")
                return 0

            logger.info(f"Merging {len(temp_contract_act_files)} temporary files into {self.contract_act_file}...")
            df_list = [pd.read_parquet(f) for f in temp_contract_act_files]
            new_df = pd.concat(df_list, ignore_index=True)
            logger.info(f"New data: {len(new_df)} records")

            existing_records = 0
            if os.path.exists(self.contract_act_file):
                existing_df = pd.read_parquet(self.contract_act_file)
                existing_records = len(existing_df)
                logger.info(f"Existing file {self.contract_act_file}: {existing_records} records")
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                logger.info(f"No existing file found for {self.contract_act_file}. Using new data only.")
                final_df = new_df

            logger.info(f"Combined data before deduplication: {len(final_df)} records")
            if self.deduplicate:
                duplicate_count = len(final_df) - len(final_df.drop_duplicates(subset=["id"]))
                final_df = final_df.drop_duplicates(subset=["id"], keep="last")
                logger.info(f"Removed {duplicate_count} duplicate records based on id")
            else:
                logger.info("Deduplication skipped (--no-deduplicate flag)")

            if existing_records > 0 and len(final_df) < existing_records:
                logger.warning(f"Final data ({len(final_df)} records) is smaller than existing data ({existing_records} records). Possible data loss!")
                raise ValueError(f"Data loss detected in {self.contract_act_file}. Aborting save.")

            final_df.to_parquet(self.contract_act_file, index=False)
            logger.info(f"Updated {self.contract_act_file} with {len(final_df)} records")

            for temp_file in temp_contract_act_files:
                os.remove(temp_file)
            if os.path.exists(self.temp_dir) and not os.listdir(self.temp_dir):
                os.rmdir(self.temp_dir)

            return len(final_df)
        except Exception as e:
            logger.error(f"Failed to merge files: {e}")
            raise

    def run(self):
        """Main execution method"""
        try:
            logger.info("Starting incremental ContractAct data collection...")
            total_loaded = self.fetch_data()
            total_contract_acts = self.merge_temp_files()
            if total_loaded == 0:
                logger.info("No new data found.")
            else:
                logger.info(f"Completed. Total contract acts fetched: {total_loaded}, saved: {total_contract_acts}")
        except Exception as e:
            logger.error(f"Error during execution: {e}")
            raise

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Goszakup ContractAct Incremental API Parser")
    parser.add_argument(
        "--token",
        required=True,
        help="API authentication token"
    )
    parser.add_argument(
        "--output-prefix",
        default="ContractAct",
        help="Prefix for output parquet file (default: ContractAct)"
    )
    parser.add_argument(
        "--no-deduplicate",
        action="store_false",
        dest="deduplicate",
        help="Disable deduplication by id (default: deduplicate enabled)"
    )
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    
    parser = ContractActIncrementalParser(
        token=args.token,
        output_prefix=args.output_prefix,
        deduplicate=args.deduplicate
    )
    
    try:
        parser.run()
    except Exception as e:
        logger.error(f"Program terminated with error: {e}")
        raise

if __name__ == "__main__":
    main()