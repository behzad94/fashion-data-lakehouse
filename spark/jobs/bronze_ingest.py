import logging
from datetime import date
from pathlib import Path

import yaml
from minio import Minio

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("bronze_ingest")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)
    return logger

def load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
    

def build_minio_client(cfg: dict) -> Minio:
    endpoint = cfg["minio"]["endpoint"].replace("http://", "").replace("https://", "")
    return Minio(
        endpoint=endpoint,
        access_key=cfg["minio"]["access_key"],
        secret_key=cfg["minio"]["secret_key"],
        secure=False,
    )

def upload_file(client: Minio, bucket: str, object_name: str, file_path: Path, logger: logging.Logger) -> None:
    logger.info(f"Uploading {file_path} -> s3://{bucket}/{object_name}")
    client.fput_object(bucket, object_name, str(file_path))
    logger.info("Upload finished")

def main() -> None:
    logger = setup_logger()

    cfg = load_config(Path("config/dev.yml"))
    client = build_minio_client(cfg)

    bucket = cfg["minio"]["bucket"]
    base = cfg["bronze"]["base_path"]
    dataset = cfg["bronze"]["dataset_name"]
    ingest_date = str(date.today())

    files_to_upload = [
        Path("data/source/online_retail/Online Retail.xlsx"),
        Path("data/source/online_retail/online_retail.csv"),
    ]
    
    for file_path in files_to_upload:
        if not file_path.exists():
            raise FileNotFoundError(f"Missing file: {file_path}")
        
        object_name = f"{base}/{dataset}/ingest_date={ingest_date}/{file_path.name}"
        upload_file(client, bucket, object_name, file_path, logger)

if __name__ == "__main__":
    main()