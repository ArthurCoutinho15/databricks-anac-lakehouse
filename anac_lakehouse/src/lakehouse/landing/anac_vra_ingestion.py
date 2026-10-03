import logging
import sys
from datetime import date
from pathlib import Path

from anac_lakehouse.clients.anac_vra_client import AnacVraBaseClient

LANDING_SCHEMA = "landing"
LANDING_VOLUME = "landing"

VRA_INDEX_PATH = "Voos e operações aéreas/Voo Regular Ativo (VRA)/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def vra_ingestion(catalog: str, start_year: int, end_year: int | None = None):
    """Baixa o VRA ano a ano, de start_year até end_year (default: ano atual)."""
    dest_dir = Path(f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}/vra")
    end_year = end_year or date.today().year
    client = AnacVraBaseClient()

    for year in range(start_year, end_year + 1):
        logger.info("Baixando VRA do ano %d", year)
        client.download_all(path=f"{VRA_INDEX_PATH}{year}/", dest_dir=dest_dir)


def main():
    catalog = sys.argv[1]
    start_year = int(sys.argv[2]) if len(sys.argv) > 2 else 2023

    vra_ingestion(catalog, start_year)


if __name__ == "__main__":
    main()
