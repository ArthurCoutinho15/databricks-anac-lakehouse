import logging
import sys
from pathlib import Path

from anac_lakehouse.clients.anp_qav_client import AnpQavClient

LANDING_SCHEMA = "landing"
LANDING_VOLUME = "landing"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def anp_ingestion(catalog: str) -> None:
    dest_dir = Path(f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}/anp_qav")
    client = AnpQavClient()
    client.download_qav_table(dest_dir=dest_dir)


def main():
    # Databricks chama main() sem argumentos quando roda via python_wheel_task;
    # os parâmetros do job chegam por sys.argv aqui dentro.
    catalog = sys.argv[1]

    anp_ingestion(catalog)


if __name__ == "__main__":
    main()
