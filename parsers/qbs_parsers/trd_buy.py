# trd_buy.py

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
        logging.FileHandler('trd_buy.log'),
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

class TrdBuyParser:
    def __init__(self, token: str, year: int, output_file: str = None):
        self.token = token
        self.year = year
        self.base_output_name = "TrdBuy"
        self.output_file = output_file if output_file else f"{self.base_output_name}_{year}.parquet"
        self.output_dir = "trdbuy_data"
        self.temp_dir = os.path.join(self.output_dir, "temp")
        self.combined_file = os.path.join(self.output_dir, self.output_file)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        self.batch_size_limit = 50000
        self.time_step = timedelta(days=7)
        self.start_date = datetime(year, 1, 1)
        self.end_date = datetime(year, 12, 31, 23, 59, 59)
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }

    def load_existing_min_date(self) -> datetime:
        """Load the minimum publishDate from the existing parquet file for the specified year"""
        try:
            if os.path.exists(self.combined_file):
                df_existing = pd.read_parquet(self.combined_file).dropna(subset=["publishDate"])
                df_existing["publishDate"] = pd.to_datetime(df_existing["publishDate"])
                # Filter for the specified year
                df_year = df_existing[df_existing["publishDate"].dt.year == self.year]
                if not df_year.empty:
                    min_publish_date = df_year["publishDate"].min()
                    logger.info(f"Found {len(df_year)} existing records for {self.year}, min publishDate: {min_publish_date}")
                    return min_publish_date - timedelta(seconds=1)
                else:
                    logger.info(f"No existing records for {self.year} in {self.combined_file}")
            else:
                logger.info(f"No existing parquet file found: {self.combined_file}")
            return self.end_date  # Start from the end of the year if no data exists
        except Exception as e:
            logger.error(f"Failed to load existing parquet file: {e}")
            return self.end_date

    def fetch_data(self) -> int:
        """Fetch TrdBuy data for the specified year"""
        start_date = self.load_existing_min_date()
        current_end_date = start_date
        trd_buy_batch = []
        batch_count = 0
        total_loaded = 0
        request_count = 0
        error_attempts = 0

        while current_end_date > self.start_date:
            current_start_date = max(current_end_date - self.time_step, self.start_date)
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

            current_end_date = current_start_date
            error_attempts = 0

        if trd_buy_batch:
            batch_count += 1
            temp_file = os.path.join(self.temp_dir, f"trd_buy_batch_{batch_count}.parquet")
            pd.DataFrame(trd_buy_batch).to_parquet(temp_file, index=False)
            logger.info(f"Saved temporary file: {temp_file} ({len(trd_buy_batch)} records)")

        return total_loaded

    def merge_files(self):
        """Merge temporary files into the final output"""
        try:
            temp_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.endswith(".parquet")]
            if not temp_files:
                logger.info("No data to merge.")
                return

            final_df = pd.concat([pd.read_parquet(f) for f in temp_files], ignore_index=True)
            if os.path.exists(self.combined_file):
                existing_df = pd.read_parquet(self.combined_file)
                final_df = pd.concat([existing_df, final_df], ignore_index=True)
                final_df = final_df.drop_duplicates(subset=["id"], keep="last")

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
            logger.info(f"Starting TrdBuy data collection for year {self.year}...")
            total_loaded = self.fetch_data()
            self.merge_files()
            if total_loaded == 0:
                logger.info(f"No data found for year {self.year}.")
            else:
                logger.info(f"Completed. Total records fetched for {self.year}: {total_loaded}")
        except Exception as e:
            logger.error(f"Error during execution: {e}")
            raise

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Goszakup TrdBuy API Parser")
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
        help="Custom output parquet file name (default: TrdBuy_<year>.parquet)"
    )
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    if args.year < 1900:
        raise ValueError("Year must be 1900 or later")
    
    parser = TrdBuyParser(
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