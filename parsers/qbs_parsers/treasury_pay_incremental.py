# treasury_pay_incremental.py

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
        logging.FileHandler('treasury_pay_incremental.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://ows.goszakup.gov.kz/v3/graphql"

QUERY_TEMPLATE = """
query GetContracts($filter: ContractFiltersInput, $after: Int) {
    Contract(filter: $filter, limit: 200, after: $after) {
        id
        crdate
        TreasuryPay {
            id
            nomZa
            contractId
            dtReg
            nomUved
            supplier
            rnnSupplier
            bikSupplier
            iikSupplier
            codeSupplier
            nomDog
            dtDog
            itemDescription
            quantity
            unitPrice
            nomDop
            dtDop
            typeBujet
            budgetNameRu
            budgetNameKz
            kato
            func
            espk
            gu
            finSource
            poHeaderId
            lastUpdateDate
            vendorId
            pdiUpdateDate
            prepaySum
            strPrepaySum
            checkId
            invoiceId
            payDescription
            invnum
            payAmount
            checkNumber
            payDate
            codeCombinationId
            ppn
            accountingDate
            systemId
            indexDate
        }
    }
}
"""

class TreasuryPayIncrementalParser:
    def __init__(self, token: str, output_prefix: str = "TreasuryPay"):
        self.token = token
        self.base_prefix = output_prefix
        self.output_dir = "contract_treasury_data"
        self.temp_dir = os.path.join(self.output_dir, "temp")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Output file
        self.treasury_file = os.path.join(self.output_dir, f"{self.base_prefix}.parquet")
        
        self.batch_size_limit = 100000
        self.time_step = timedelta(days=7)
        self.end_date = datetime.now()
        self.start_date = datetime(2024, 1, 1)  # Fallback start date
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}"
        }
        self.global_error_attempts = 0

    def load_existing_max_date(self) -> datetime:
        """Load the maximum contract_crdate from the existing parquet file and temp files"""
        try:
            max_date = None
            # Check main treasury pays file
            if os.path.exists(self.treasury_file):
                df_existing = pd.read_parquet(self.treasury_file).dropna(subset=["contract_crdate"])
                logger.info(f"Loaded existing file with {len(df_existing)} records")
                if not df_existing.empty and "contract_crdate" in df_existing.columns:
                    df_existing["contract_crdate"] = pd.to_datetime(df_existing["contract_crdate"], errors='coerce')
                    valid_dates = df_existing["contract_crdate"].dropna()
                    if not valid_dates.empty:
                        max_date = valid_dates.max()
                        logger.info(f"Max contract_crdate in {self.treasury_file}: {max_date}, valid date count: {len(valid_dates)}")
                    else:
                        logger.warning("No valid contract_crdate values in existing file")
                else:
                    logger.warning("Existing file is empty or missing contract_crdate column")

            # Check temporary files
            temp_treasury_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.startswith("treasury_batch_") and f.endswith(".parquet")]
            for temp_file in temp_treasury_files:
                temp_df = pd.read_parquet(temp_file).dropna(subset=["contract_crdate"])
                if not temp_df.empty and "contract_crdate" in temp_df.columns:
                    temp_df["contract_crdate"] = pd.to_datetime(temp_df["contract_crdate"], errors='coerce')
                    valid_dates = temp_df["contract_crdate"].dropna()
                    if not valid_dates.empty:
                        temp_max_date = valid_dates.max()
                        if max_date is None or (temp_max_date is not None and temp_max_date > max_date):
                            max_date = temp_max_date
                            logger.info(f"Updated max contract_crdate from temp file {temp_file}: {max_date}")

            if max_date is not None:
                return max_date + timedelta(seconds=1)
            else:
                logger.info(f"No valid contract_crdate found. Starting from {self.start_date}")
                return self.start_date
        except Exception as e:
            logger.error(f"Failed to load existing data: {e}")
            return self.start_date

    def fetch_data(self) -> tuple[int, int]:
        """Fetch new TreasuryPay data incrementally"""
        current_start_date = self.load_existing_max_date()
        treasury_batch = []
        batch_count = 0
        total_loaded_contracts = 0
        total_loaded_treasury = 0
        request_count = 0
        error_attempts = 0

        # Initialize batch_count based on existing temporary files
        existing_temp_files = [f for f in os.listdir(self.temp_dir) if f.startswith("treasury_batch_") and f.endswith(".parquet")]
        for f in existing_temp_files:
            try:
                batch_number = int(f.replace("treasury_batch_", "").replace(".parquet", ""))
                batch_count = max(batch_count, batch_number)
            except ValueError:
                continue

        while current_start_date < self.end_date:
            current_end_date = min(current_start_date + self.time_step, self.end_date)
            variables = {
                "filter": {
                    "crdate": [
                        current_start_date.strftime("%Y-%m-%d %H:%M:%S"),
                        current_end_date.strftime("%Y-%m-%d %H:%M:%S")
                    ]
                },
                "after": None
            }
            
            logger.info(f"Fetching data for contract_crdate {current_start_date} to {current_end_date}")
            
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
                        self.global_error_attempts += 1
                        if error_attempts >= 5 or self.global_error_attempts >= 10:
                            logger.error("Reached error limit. Stopping.")
                            return total_loaded_contracts, total_loaded_treasury
                        time.sleep(5)
                        break

                    data_content = data.get("data")
                    if not isinstance(data_content, dict):
                        logger.error(f"Invalid response: 'data' is not a dictionary")
                        error_attempts += 1
                        self.global_error_attempts += 1
                        if error_attempts >= 5 or self.global_error_attempts >= 10:
                            logger.error("Reached error limit. Stopping.")
                            return total_loaded_contracts, total_loaded_treasury
                        time.sleep(5)
                        break

                    contracts = data_content.get("Contract")
                    if contracts is None:
                        logger.warning(f"No contracts found for date range {current_start_date} to {current_end_date}")
                        error_attempts += 1
                        self.global_error_attempts += 1
                        if error_attempts >= 5 or self.global_error_attempts >= 10:
                            logger.error("Reached error limit. Stopping.")
                            return total_loaded_contracts, total_loaded_treasury
                        time.sleep(5)
                        break

                    if not isinstance(contracts, list):
                        logger.error(f"Contracts is not a list: {contracts}")
                        error_attempts += 1
                        self.global_error_attempts += 1
                        if error_attempts >= 5 or self.global_error_attempts >= 10:
                            logger.error("Reached error limit. Stopping.")
                            return total_loaded_contracts, total_loaded_treasury
                        time.sleep(5)
                        break

                    if not contracts:
                        logger.info(f"Empty contracts list for date range {current_start_date} to {current_end_date}")
                        break

                    count_loaded_contracts = 0
                    count_loaded_treasury = 0
                    for contract in contracts:
                        count_loaded_contracts += 1
                        treasury_pays = contract.get("TreasuryPay", [])
                        if treasury_pays is None:
                            logger.debug(f"Skipping contract {contract['id']} with null TreasuryPay")
                            continue
                        for treasury in treasury_pays:
                            treasury_batch.append({
                                "id": treasury["id"],
                                "contract_id": contract["id"],
                                "contract_crdate": contract["crdate"],
                                "nomZa": treasury["nomZa"],
                                "contractId": treasury["contractId"],
                                "dtReg": treasury["dtReg"],
                                "nomUved": treasury["nomUved"],
                                "supplier": treasury["supplier"],
                                "rnnSupplier": treasury["rnnSupplier"],
                                "bikSupplier": treasury["bikSupplier"],
                                "iikSupplier": treasury["iikSupplier"],
                                "codeSupplier": treasury["codeSupplier"],
                                "nomDog": treasury["nomDog"],
                                "dtDog": treasury["dtDog"],
                                "itemDescription": treasury["itemDescription"],
                                "quantity": treasury["quantity"],
                                "unitPrice": treasury["unitPrice"],
                                "nomDop": treasury["nomDop"],
                                "dtDop": treasury["dtDop"],
                                "typeBujet": treasury["typeBujet"],
                                "budgetNameRu": treasury["budgetNameRu"],
                                "budgetNameKz": treasury["budgetNameKz"],
                                "kato": treasury["kato"],
                                "func": treasury["func"],
                                "espk": treasury["espk"],
                                "gu": treasury["gu"],
                                "finSource": treasury["finSource"],
                                "poHeaderId": treasury["poHeaderId"],
                                "lastUpdateDate": treasury["lastUpdateDate"],
                                "vendorId": treasury["vendorId"],
                                "pdiUpdateDate": treasury["pdiUpdateDate"],
                                "prepaySum": treasury["prepaySum"],
                                "strPrepaySum": treasury["strPrepaySum"],
                                "checkId": treasury["checkId"],
                                "invoiceId": treasury["invoiceId"],
                                "payDescription": treasury["payDescription"],
                                "invnum": treasury["invnum"],
                                "payAmount": treasury["payAmount"],
                                "checkNumber": treasury["checkNumber"],
                                "payDate": treasury["payDate"],
                                "codeCombinationId": treasury["codeCombinationId"],
                                "ppn": treasury["ppn"],
                                "accountingDate": treasury["accountingDate"],
                                "systemId": treasury["systemId"],
                                "indexDate": treasury["indexDate"]
                            })
                            count_loaded_treasury += 1
                    
                    total_loaded_contracts += count_loaded_contracts
                    total_loaded_treasury += count_loaded_treasury
                    logger.info(f"Loaded: {count_loaded_contracts} contracts, {count_loaded_treasury} treasury pays. Total loaded: {total_loaded_treasury}")

                    if len(treasury_batch) >= self.batch_size_limit:
                        batch_count += 1
                        temp_treasury_file = os.path.join(self.temp_dir, f"treasury_batch_{batch_count}.parquet")
                        pd.DataFrame(treasury_batch).to_parquet(temp_treasury_file, index=False)
                        logger.info(f"Saved temporary file: {temp_treasury_file} ({len(treasury_batch)} records)")
                        treasury_batch = []

                    page_info = data.get("extensions", {}).get("pageInfo", {})
                    if not page_info.get("hasNextPage", False):
                        break
                    variables["after"] = page_info.get("lastId")
                    if variables["after"] is None:
                        logger.warning(f"No lastId in page_info, stopping pagination")
                        break

                    error_attempts = 0
                    time.sleep(1)

                except requests.exceptions.Timeout:
                    logger.warning("Request timeout. Retrying in 5 seconds...")
                    time.sleep(5)
                    error_attempts += 1
                    self.global_error_attempts += 1
                    if error_attempts >= 5 or self.global_error_attempts >= 10:
                        logger.error("Reached error limit. Stopping.")
                        return total_loaded_contracts, total_loaded_treasury
                except Exception as e:
                    logger.error(f"Error: {e}")
                    error_attempts += 1
                    self.global_error_attempts += 1
                    if error_attempts >= 5 or self.global_error_attempts >= 10:
                        logger.error("Reached error limit. Stopping.")
                        return total_loaded_contracts, total_loaded_treasury
                    time.sleep(5)

            current_start_date = current_end_date + timedelta(seconds=1)
            error_attempts = 0

        if treasury_batch:
            batch_count += 1
            temp_treasury_file = os.path.join(self.temp_dir, f"treasury_batch_{batch_count}.parquet")
            pd.DataFrame(treasury_batch).to_parquet(temp_treasury_file, index=False)
            logger.info(f"Saved temporary file: {temp_treasury_file} ({len(treasury_batch)} records)")

        return total_loaded_contracts, total_loaded_treasury

    def merge_temp_files(self):
        """Merge temporary files into the final output"""
        try:
            temp_treasury_files = [os.path.join(self.temp_dir, f) for f in os.listdir(self.temp_dir) if f.startswith("treasury_batch_") and f.endswith(".parquet")]
            if not temp_treasury_files:
                logger.info(f"No new data to merge for {self.treasury_file}. Existing file unchanged.")
                return 0

            logger.info(f"Merging {len(temp_treasury_files)} temporary files into {self.treasury_file}...")
            df_list = [pd.read_parquet(f) for f in temp_treasury_files]
            new_df = pd.concat(df_list, ignore_index=True)
            logger.info(f"New data: {len(new_df)} records")

            existing_records = 0
            if os.path.exists(self.treasury_file):
                existing_df = pd.read_parquet(self.treasury_file)
                existing_records = len(existing_df)
                logger.info(f"Existing file {self.treasury_file}: {existing_records} records")
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                logger.info(f"No existing file found for {self.treasury_file}. Using new data only.")
                final_df = new_df

            logger.info(f"Combined data before deduplication: {len(final_df)} records")
            duplicate_count = len(final_df) - len(final_df.drop_duplicates(subset=["id", "contract_id"]))
            final_df = final_df.drop_duplicates(subset=["id", "contract_id"], keep="last")
            logger.info(f"Removed {duplicate_count} duplicate records based on id, contract_id")

            if existing_records > 0 and len(final_df) < existing_records:
                logger.warning(f"Final data ({len(final_df)} records) is smaller than existing data ({existing_records} records). Possible data loss!")
                raise ValueError(f"Data loss detected in {self.treasury_file}. Aborting save.")

            final_df.to_parquet(self.treasury_file, index=False)
            logger.info(f"Updated {self.treasury_file} with {len(final_df)} records")

            for temp_file in temp_treasury_files:
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
            logger.info("Starting incremental TreasuryPay data collection...")
            total_loaded_contracts, total_loaded_treasury = self.fetch_data()
            total_treasury = self.merge_temp_files()
            if total_loaded_treasury == 0:
                logger.info("No new treasury pays found.")
            else:
                logger.info(f"Completed. Total contracts fetched: {total_loaded_contracts}, treasury pays fetched: {total_loaded_treasury}, saved: {total_treasury}")
        except Exception as e:
            logger.error(f"Error during execution: {e}")
            raise

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Goszakup TreasuryPay Incremental API Parser")
    parser.add_argument(
        "--token",
        required=True,
        help="API authentication token"
    )
    parser.add_argument(
        "--output-prefix",
        default="TreasuryPay",
        help="Prefix for output parquet file (default: TreasuryPay)"
    )
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    
    parser = TreasuryPayIncrementalParser(
        token=args.token,
        output_prefix=args.output_prefix
    )
    
    try:
        parser.run()
    except Exception as e:
        logger.error(f"Program terminated with error: {e}")
        raise

if __name__ == "__main__":
    main()