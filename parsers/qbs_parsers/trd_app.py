# trd_app.py

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
        logging.FileHandler('trd_app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://ows.goszakup.gov.kz/v3/graphql"

QUERY_TEMPLATE = """
query GetTrdApps($filter: TrdAppFiltersInput, $after: Int) {
    TrdApp(filter: $filter, limit: 200, after: $after) {
        id
        buyId
        supplierId
        crFio
        modFio
        supplierBinIin
        protId
        protNumber
        dateApply
        AppLots {
            id
            lotId
            pointList
            statusId
            price
            amount
            discountValue
            discountPrice
            Offers {
                id
                pointId
                lotId
                appLotId
                price
                amount
            }
        }
    }
}
"""

class TrdAppParser:
    def __init__(self, token: str, year: int, output_prefix: str = None):
        self.token = token
        self.year = year
        self.base_prefix = output_prefix if output_prefix else "TrdApp"
        self.output_dir = "trdapp_data"
        self.temp_dir = os.path.join(self.output_dir, "temp")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Output files with year appended
        self.trdapps_file = os.path.join(self.output_dir, f"{self.base_prefix}_{year}.parquet")
        self.lots_file = os.path.join(self.output_dir, f"{self.base_prefix}Lots_{year}.parquet")
        self.offers_file = os.path.join(self.output_dir, f"{self.base_prefix}PriceOfferPoint_{year}.parquet")
        
        self.batch_size_limit = 100000
        self.time_step = timedelta(days=7)
        self.start_date = datetime(year, 1, 1)
        self.end_date = datetime(year, 12, 31, 23, 59, 59)
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }

    def load_existing_min_date(self) -> datetime:
        """Load the minimum dateApply from the existing TrdApp parquet file for the specified year"""
        try:
            if os.path.exists(self.trdapps_file):
                df_existing = pd.read_parquet(self.trdapps_file).dropna(subset=["dateApply"])
                df_existing["dateApply"] = pd.to_datetime(df_existing["dateApply"])
                # Filter for the specified year
                df_year = df_existing[df_existing["dateApply"].dt.year == self.year]
                if not df_year.empty:
                    min_date_apply = df_year["dateApply"].min()
                    logger.info(f"Found {len(df_year)} existing records for {self.year}, min dateApply: {min_date_apply}")
                    return min_date_apply - timedelta(seconds=1)
                else:
                    logger.info(f"No existing records for {self.year} in {self.trdapps_file}")
            else:
                logger.info(f"No existing parquet file found: {self.trdapps_file}")
            return self.end_date  # Start from the end of the year if no data exists
        except Exception as e:
            logger.error(f"Failed to load existing parquet file: {e}")
            return self.end_date

    def fetch_data(self) -> int:
        """Fetch TrdApp data for the specified year"""
        start_date = self.load_existing_min_date()
        current_end_date = start_date
        trdapps_batch = []
        lots_batch = []
        offers_batch = []
        batch_count = 0
        total_loaded = 0
        request_count = 0
        error_attempts = 0

        while current_end_date > self.start_date:
            current_start_date = max(current_end_date - self.time_step, self.start_date)
            variables = {
                "filter": {
                    "dateApply": {
                        "gte": current_start_date.strftime("%Y-%m-%d %H:%M:%S") + ".000000",
                        "lte": current_end_date.strftime("%Y-%m-%d %H:%M:%S") + ".000000"
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

                    trdapps = data.get("data", {}).get("TrdApp")
                    if trdapps is None:
                        logger.warning("TrdApp returned None. Moving to next interval.")
                        break
                    if not trdapps:
                        logger.info("No data for this interval or page ended.")
                        break

                    count_loaded = 0
                    for trdapp in trdapps:
                        trdapps_batch.append({
                            "id": trdapp["id"],
                            "buyId": trdapp["buyId"],
                            "supplierId": trdapp["supplierId"],
                            "crFio": trdapp["crFio"],
                            "modFio": trdapp["modFio"],
                            "supplierBinIin": trdapp["supplierBinIin"],
                            "protId": trdapp["protId"],
                            "protNumber": trdapp["protNumber"],
                            "dateApply": trdapp["dateApply"]
                        })

                        app_lots = trdapp.get("AppLots")
                        if app_lots is None:
                            continue

                        for lot in app_lots:
                            lots_batch.append({
                                "trdapp_id": trdapp["id"],
                                "id": lot["id"],
                                "lotId": lot["lotId"],
                                "pointList": lot["pointList"],
                                "statusId": lot["statusId"],
                                "price": lot["price"],
                                "amount": lot["amount"],
                                "discountValue": lot["discountValue"],
                                "discountPrice": lot["discountPrice"]
                            })

                            offers = lot.get("Offers")
                            if offers is None:
                                continue

                            for offer in offers:
                                offers_batch.append({
                                    "lot_id": lot["id"],
                                    "id": offer["id"],
                                    "pointId": offer["pointId"],
                                    "lotId": offer["lotId"],
                                    "appLotId": offer["appLotId"],
                                    "price": offer["price"],
                                    "amount": offer["amount"]
                                })

                            count_loaded += 1
                    
                    total_loaded += count_loaded
                    logger.info(f"Loaded {count_loaded} records. Total loaded: {total_loaded}")
                    if trdapps:
                        logger.info(f"Last dateApply in response: {trdapps[-1]['dateApply']}")

                    # Save batches if limit reached
                    if len(trdapps_batch) >= self.batch_size_limit:
                        batch_count += 1
                        temp_trdapps_file = os.path.join(self.temp_dir, f"trdapps_batch_{batch_count}.parquet")
                        pd.DataFrame(trdapps_batch).to_parquet(temp_trdapps_file, index=False)
                        logger.info(f"Saved temporary trdapps file: {temp_trdapps_file} ({len(trdapps_batch)} records)")
                        trdapps_batch = []

                    if len(lots_batch) >= self.batch_size_limit:
                        batch_count += 1
                        temp_lots_file = os.path.join(self.temp_dir, f"lots_batch_{batch_count}.parquet")
                        pd.DataFrame(lots_batch).to_parquet(temp_lots_file, index=False)
                        logger.info(f"Saved temporary lots file: {temp_lots_file} ({len(lots_batch)} records)")
                        lots_batch = []

                    if len(offers_batch) >= self.batch_size_limit:
                        batch_count += 1
                        temp_offers_file = os.path.join(self.temp_dir, f"offers_batch_{batch_count}.parquet")
                        pd.DataFrame(offers_batch).to_parquet(temp_offers_file, index=False)
                        logger.info(f"Saved temporary offers file: {temp_offers_file} ({len(offers_batch)} records)")
                        offers_batch = []

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

        # Save remaining batches
        if trdapps_batch:
            batch_count += 1
            temp_trdapps_file = os.path.join(self.temp_dir, f"trdapps_batch_{batch_count}.parquet")
            pd.DataFrame(trdapps_batch).to_parquet(temp_trdapps_file, index=False)
            logger.info(f"Saved temporary trdapps file: {temp_trdapps_file} ({len(trdapps_batch)} records)")

        if lots_batch:
            batch_count += 1
            temp_lots_file = os.path.join(self.temp_dir, f"lots_batch_{batch_count}.parquet")
            pd.DataFrame(lots_batch).to_parquet(temp_lots_file, index=False)
            logger.info(f"Saved temporary lots file: {temp_lots_file} ({len(lots_batch)} records)")

        if offers_batch:
            batch_count += 1
            temp_offers_file = os.path.join(self.temp_dir, f"offers_batch_{batch_count}.parquet")
            pd.DataFrame(offers_batch).to_parquet(temp_offers_file, index=False)
            logger.info(f"Saved temporary offers file: {temp_offers_file} ({len(offers_batch)} records)")

        return total_loaded

    def merge_temp_files(self, temp_files: list, output_file: str, key_columns: list) -> int:
        """Merge temporary files into the output file, deduplicating by key columns"""
        try:
            if not temp_files:
                logger.info(f"No new data to merge for {output_file}.")
                return 0

            logger.info(f"Merging {len(temp_files)} temporary files into {output_file}...")
            df_list = [pd.read_parquet(f) for f in temp_files]
            all_data = pd.concat(df_list, ignore_index=True)
            existing_records = 0
            if os.path.exists(output_file):
                existing_df = pd.read_parquet(output_file)
                existing_records = len(existing_df)
                logger.info(f"Existing file {output_file}: {existing_records} records")
                all_data = pd.concat([existing_df, all_data], ignore_index=True)
            else:
                logger.info(f"No existing file found for {output_file}. Using new data only.")

            logger.info(f"Combined data before deduplication: {len(all_data)} records")
            duplicate_count = len(all_data) - len(all_data.drop_duplicates(subset=key_columns))
            all_data = all_data.drop_duplicates(subset=key_columns, keep="last")
            logger.info(f"Removed {duplicate_count} duplicate records based on {key_columns}")

            if existing_records > 0 and len(all_data) < existing_records:
                logger.warning(f"Final data ({len(all_data)} records) is smaller than existing data ({existing_records} records) for {output_file}. Possible data loss!")
                raise ValueError(f"Data loss detected in {output_file}. Aborting save.")

            all_data.to_parquet(output_file, index=False)
            logger.info(f"Updated {output_file} with {len(all_data)} records")
            
            for temp_file in temp_files:
                os.remove(temp_file)
            return len(all_data)
        except Exception as e:
            logger.error(f"Failed to merge files for {output_file}: {e}")
            raise

    def merge_files(self):
        """Merge temporary files for all tables"""
        temp_trdapps_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.startswith("trdapps_batch_") and f.endswith(".parquet")]
        temp_lots_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.startswith("lots_batch_") and f.endswith(".parquet")]
        temp_offers_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.startswith("offers_batch_") and f.endswith(".parquet")]

        total_trdapps = self.merge_temp_files(temp_trdapps_files, self.trdapps_file, ["id"])
        total_lots = self.merge_temp_files(temp_lots_files, self.lots_file, ["trdapp_id", "id"])
        total_offers = self.merge_temp_files(temp_offers_files, self.offers_file, ["lot_id", "id"])

        if os.path.exists(self.temp_dir) and not os.listdir(self.temp_dir):
            os.rmdir(self.temp_dir)

        logger.info("Temporary files cleaned up.")
        logger.info("Final data counts:")
        logger.info(f"  - TrdApp: {total_trdapps} records")
        logger.info(f"  - TrdAppLots: {total_lots} records")
        logger.info(f"  - TrdPriceOfferPoint: {total_offers} records")

    def run(self):
        """Main execution method"""
        try:
            logger.info(f"Starting TrdApp data collection for year {self.year}...")
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
    parser = argparse.ArgumentParser(description="Goszakup TrdApp API Parser")
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
        "--output-prefix",
        help="Custom prefix for output parquet files (default: TrdApp)"
    )
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    if args.year < 1900:
        raise ValueError("Year must be 1900 or later")
    
    parser = TrdAppParser(
        token=args.token,
        year=args.year,
        output_prefix=args.output_prefix
    )
    
    try:
        parser.run()
    except Exception as e:
        logger.error(f"Program terminated with error: {e}")
        raise

if __name__ == "__main__":
    main()