# trd_buy_incremental.py

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
        logging.FileHandler('trd_buy_incremental.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://ows.goszakup.gov.kz/v3/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer d5c3d78fc111d88a0a37b4ab8f83cbd5"
}

QUERY_TEMPLATE = """
query GetTrdBuy($filter: TrdBuyFiltersInput, $after: Int) {
    TrdBuy(filter: $filter, limit: 200, after: $after) {
        id
        numberAnno
        nameRu
        nameKz
        totalSum
        countLots
        refTradeMethodsId
        refSubjectTypeId
        customerBin
        customerPid
        customerNameKz
        customerNameRu
        orgBin
        orgPid
        orgNameKz
        orgNameRu
        refBuyStatusId
        startDate
        repeatStartDate
        repeatEndDate
        endDate
        publishDate
        itogiDatePublic
        refTypeTradeId
        disablePersonId
        discusStartDate
        discusEndDate
        idSupplier
        biinSupplier
        parentId
        singlOrgSign
        isLightIndustry
        isConstructionWork
        refSpecPurchaseTypeId
        lastUpdateDate
        finYear
        kato
        systemId
        indexDate
    }
}
"""

class TrdBuyIncrementalParser:
    def __init__(self, token: str, output_file: str = "trdBuy.parquet", deduplicate: bool = True):
        self.token = token
        self.output_file = output_file
        self.deduplicate = deduplicate
        self.output_dir = "trdbuy_data"
        self.temp_dir = os.path.join(self.output_dir, "temp")
        self.combined_file = os.path.join(self.output_dir, self.output_file)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        self.batch_size_limit = 50000
        self.time_step = timedelta(days=7)
        self.end_date = datetime.now()
        self.start_date = datetime(2024, 1, 1)  # Fallback start date
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }

    def load_existing_max_date(self) -> datetime:
        """Load the maximum publishDate from the existing parquet file"""
        try:
            if os.path.exists(self.combined_file):
                df_existing = pd.read_parquet(self.combined_file)
                logger.info(f"Loaded existing file with {len(df_existing)} records")
                if not df_existing.empty and "publishDate" in df_existing.columns:
                    df_existing["publishDate"] = pd.to_datetime(df_existing["publishDate"], errors='coerce')
                    valid_dates = df_existing["publishDate"].dropna()
                    if not valid_dates.empty:
                        max_publish_date = valid_dates.max()
                        logger.info(f"Max publishDate: {max_publish_date}, valid publishDate count: {len(valid_dates)}")
                        return max_publish_date + timedelta(seconds=1)
                    else:
                        logger.warning("No valid publishDate values in existing file")
                else:
                    logger.warning("Existing file is empty or missing publishDate column")
            else:
                logger.info(f"No existing parquet file found: {self.combined_file}")
            return self.start_date
        except Exception as e:
            logger.error(f"Failed to load existing parquet file: {e}")
            return self.start_date

    def fetch_data(self) -> int:
        """Fetch new TrdBuy data incrementally"""
        current_start_date = self.load_existing_max_date()
        trd_buy_batch = []
        batch_count = 0
        total_loaded = 0
        request_count = 0
        error_attempts = 0

        while current_start_date < self.end_date:
            current_end_date = min(current_start_date + self.time_step, self.end_date)
            variables = {
                "filter": {
                    "publishDate": {
                        "gte": current_start_date.strftime("%Y-%m-%d %H:%M:%S"),
                        "lte": current_end_date.strftime("%Y-%m-%d %H:%M:%S")
                    }
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

                    trd_buy = data.get("data", {}).get("TrdBuy")
                    if trd_buy is None:
                        logger.warning("TrdBuy returned None. Moving to next interval.")
                        break
                    if not trd_buy:
                        logger.info("No data for this interval or page ended.")
                        break

                    count_loaded = 0
                    for item in trd_buy:
                        trd_buy_batch.append({
                            "id": item["id"],
                            "numberAnno": item["numberAnno"],
                            "nameRu": item["nameRu"],
                            "nameKz": item["nameKz"],
                            "totalSum": item["totalSum"],
                            "countLots": item["countLots"],
                            "refTradeMethodsId": item["refTradeMethodsId"],
                            "refSubjectTypeId": item["refSubjectTypeId"],
                            "customerBin": item["customerBin"],
                            "customerPid": item["customerPid"],
                            "customerNameKz": item["customerNameKz"],
                            "customerNameRu": item["customerNameRu"],
                            "orgBin": item["orgBin"],
                            "orgPid": item["orgPid"],
                            "orgNameKz": item["orgNameKz"],
                            "orgNameRu": item["orgNameRu"],
                            "refBuyStatusId": item["refBuyStatusId"],
                            "startDate": item["startDate"],
                            "repeatStartDate": item["repeatStartDate"],
                            "repeatEndDate": item["repeatEndDate"],
                            "endDate": item["endDate"],
                            "publishDate": item["publishDate"],
                            "itogiDatePublic": item["itogiDatePublic"],
                            "refTypeTradeId": item["refTypeTradeId"],
                            "disablePersonId": item["disablePersonId"],
                            "discusStartDate": item["discusStartDate"],
                            "discusEndDate": item["discusEndDate"],
                            "idSupplier": item["idSupplier"],
                            "biinSupplier": item["biinSupplier"],
                            "parentId": item.get("parentId", None),
                            "singlOrgSign": item["singlOrgSign"],
                            "isLightIndustry": item["isLightIndustry"],
                            "isConstructionWork": item["isConstructionWork"],
                            "refSpecPurchaseTypeId": item["refSpecPurchaseTypeId"],
                            "lastUpdateDate": item["lastUpdateDate"],
                            "finYear": item["finYear"],
                            "kato": item["kato"],
                            "systemId": item["systemId"],
                            "indexDate": item["indexDate"]
                        })
                        count_loaded += 1

                    total_loaded += count_loaded
                    logger.info(f"Loaded {count_loaded} records. Total loaded: {total_loaded}")

                    if len(trd_buy_batch) >= self.batch_size_limit:
                        batch_count += 1
                        temp_file = os.path.join(self.temp_dir, f"trd_buy_batch_{batch_count}.parquet")
                        pd.DataFrame(trd_buy_batch).to_parquet(temp_file, index=False)
                        logger.info(f"Saved temporary file: {temp_file} ({len(trd_buy_batch)} records)")
                        trd_buy_batch = []

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

            current_start_date = current_end_date
            error_attempts = 0

        if trd_buy_batch:
            batch_count += 1
            temp_file = os.path.join(self.temp_dir, f"trd_buy_batch_{batch_count}.parquet")
            pd.DataFrame(trd_buy_batch).to_parquet(temp_file, index=False)
            logger.info(f"Saved temporary file: {temp_file} ({len(trd_buy_batch)} records)")

        return total_loaded

    def merge_files(self):
        """Merge temporary files with existing data into the final output"""
        try:
            temp_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.endswith(".parquet")]
            if not temp_files:
                logger.info("No new data to merge. Existing file unchanged.")
                return

            new_df = pd.concat([pd.read_parquet(f) for f in temp_files], ignore_index=True)
            logger.info(f"New data: {len(new_df)} records")

            existing_records = 0
            if os.path.exists(self.combined_file):
                existing_df = pd.read_parquet(self.combined_file)
                existing_records = len(existing_df)
                logger.info(f"Existing file: {existing_records} records")
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                logger.info("No existing file found. Using new data only.")
                final_df = new_df

            logger.info(f"Combined data before deduplication: {len(final_df)} records")
            if self.deduplicate:
                duplicate_count = len(final_df) - len(final_df.drop_duplicates(subset=["id"]))
                final_df = final_df.drop_duplicates(subset=["id"], keep="last")
                logger.info(f"Removed {duplicate_count} duplicate records based on id")
            else:
                logger.info("Deduplication skipped (--no-deduplicate flag)")

            if existing_records > 0 and len(final_df) < existing_records:
                logger.warning(f"Final data ({len(final_df)} records) is smaller than existing data ({existing_records} records). Possible data loss detected!")
                raise ValueError("Final data is smaller than existing data. Aborting save to prevent data loss.")

            final_df.to_parquet(self.combined_file, index=False)
            logger.info(f"Combined file saved: {self.combined_file} ({len(final_df)} records)")

            for f in temp_files:
                os.remove(f)
            if os.path.exists(self.temp_dir) and not os.listdir(self.temp_dir):
                os.rmdir(self.temp_dir)

        except Exception as e:
            logger.error(f"Failed to merge files: {e}")
            raise

    def run(self):
        """Main execution method"""
        try:
            logger.info("Starting incremental TrdBuy data collection...")
            total_loaded = self.fetch_data()
            self.merge_files()
            if total_loaded == 0:
                logger.info("No new data found.")
            else:
                logger.info(f"Completed. Total new records fetched: {total_loaded}")
        except Exception as e:
            logger.error(f"Error during execution: {e}")
            raise

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Goszakup TrdBuy Incremental API Parser")
    parser.add_argument(
        "--token",
        required=True,
        help="API authentication token"
    )
    parser.add_argument(
        "--output",
        default="trdBuy.parquet",
        help="Output parquet file name (default: trdBuy.parquet)"
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
    
    parser = TrdBuyIncrementalParser(
        token=args.token,
        output_file=args.output,
        deduplicate=args.deduplicate
    )
    
    try:
        parser.run()
    except Exception as e:
        logger.error(f"Program terminated with error: {e}")
        raise

if __name__ == "__main__":
    main()