import logging
from datetime import UTC, datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


class OurAirportsClient:
    """Client para baixar o snapshot diário de aeroportos do OurAirports."""

    def __init__(
        self,
        url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv",
        timeout: int = 60,
    ):
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def download(self, dest_dir: Path, overwrite: bool = False) -> Path:
        """Baixa o snapshot do dia para dest_dir/dt=YYYY-MM-DD/airports.csv.

        Particiona por data de ingestão (a fonte não tem data própria -- é sempre o
        estado atual) para o Auto Loader enxergar cada dia como um arquivo novo e a
        ingestão continuar incremental/idempotente.
        """
        today = datetime.now(tz=UTC).date().isoformat()
        partition_dir = dest_dir / f"dt={today}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        dest_path = partition_dir / "airports.csv"

        if dest_path.exists() and not overwrite:
            logger.info("Já existe, pulando: %s", dest_path)
            return dest_path

        logger.info("Baixando %s -> %s", self.url, dest_path)
        with self.session.get(self.url, timeout=self.timeout, stream=True) as response:
            response.raise_for_status()
            with dest_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        logger.info("Baixado: %s", dest_path)
        return dest_path
