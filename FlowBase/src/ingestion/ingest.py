import pandas as pd
import boto3
import requests
from io import StringIO
import os
from utils.logger import PipeLineLogger

class IngestData:
    def __init__(self, config: dict, source_type: str):
        self.config = config
        self.source = source_type
        self.logger = PipeLineLogger(self.__class__.__name__).get_logger()

    def ingest(self):
        if self.source == "s3" and self.config.get("enabled", False):
            return self._ingest_s3()
        elif self.source == "local":
            return self._ingest_local()
        elif self.source == "api" and self.config.get("enabled", False):
            return self._ingest_api()
        else:
            self.logger.error("Invalid source or source disabled in config.")
            return pd.DataFrame()

    def _ingest_s3(self):
        s3 = boto3.client("s3")
        bucket = self.config.get("bucket")
        prefix = self.config.get("prefix")

        self.logger.info(f"Ingesting objects from bucket: {bucket} and prefix: {prefix}")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get("Contents", [])

        if not contents:
            self.logger.error(f"No files found in bucket: {bucket} with prefix: {prefix}")
            return pd.DataFrame()

        dfs = []
        for obj in contents:
            key = obj["Key"]
            if key.endswith(".csv"):
                self.logger.info(f"Ingesting file {key} from S3")
                s3_obj = s3.get_object(Bucket=bucket, Key=key)
                data = s3_obj["Body"].read().decode("utf-8")
                df = pd.read_csv(StringIO(data))
                dfs.append(df)

        if not dfs:
            self.logger.warning("No CSV files ingested from S3.")
            return pd.DataFrame()

        combined_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Successfully ingested: {len(dfs)} files, total rows: {len(combined_df)}")
        return combined_df

    def _ingest_local(self):
        path = self.config.get("file_path")
        if not os.path.exists(path):
            self.logger.error(f"Local file {path} does not exist.")
            return pd.DataFrame()

        self.logger.info(f"Reading local file: {path}")
        df = pd.read_csv(path)
        self.logger.info(f"Successfully read: {len(df)} rows from {path}")
        return df

    def _ingest_api(self):
        url = self.config.get("url")
        token = self.config.get("auth_token", "")
        headers = {"Authorization": f"Bearer {token}"} if token else {}

        self.logger.info(f"Fetching data from API: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code != 200:
                self.logger.warning(f"API request failed with status: {response.status_code}")
                return pd.DataFrame()

            data = response.json()
            df = pd.DataFrame(data)
            self.logger.info(f"Successfully ingested {len(df)} rows from API")
            return df
        except Exception as e:
            self.logger.error(f"API request exception: {e}")
            return pd.DataFrame()
