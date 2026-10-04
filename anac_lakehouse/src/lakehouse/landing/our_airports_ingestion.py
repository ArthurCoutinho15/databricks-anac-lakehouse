import logging
import sys
from pathlib import Path

from anac_lakehouse.clients.our_airports_client import OurAirportsClient

LANDING_SCHEMA = "landing"
LANDING_VOLUME = "landing"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def our_airports_ingestion(catalog: str):
    dest_dir = Path(f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}/ourairports")
    client = OurAirportsClient()
    client.download(dest_dir=dest_dir)


def main():
    # Databricks chama main() sem argumentos quando roda via python_wheel_task;
    # os parâmetros do job chegam por sys.argv aqui dentro.
    catalog = sys.argv[1]
    our_airports_ingestion(catalog)


if __name__ == "__main__":
    main()
