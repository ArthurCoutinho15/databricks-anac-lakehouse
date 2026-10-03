import logging
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Nomes de arquivo da VRA trazem ano/mês, ex.: VRA_2023_01.csv, vra-2023-1.csv
_YEAR_MONTH_PATTERN = re.compile(r"(?P<year>20\d{2})[_-]?(?P<month>0?[1-9]|1[0-2])(?!\d)")


class AnacVraBaseClient:
    """Client para listar e baixar os arquivos mensais do VRA (ANAC dados abertos)."""

    def __init__(self, base_url: str = "https://sistemas.anac.gov.br/dadosabertos/", timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})

    def list_files(self, path: str, extension: str = ".csv", max_depth: int = 6) -> list[str]:
        """Lista recursivamente as URLs de arquivos com a extensão informada a partir de uma página de índice.

        O índice do ANAC tem subpastas (ano/mês), então qualquer link que termine em
        '/' é tratado como subpasta e percorrido recursivamente até `max_depth`.
        """
        index_url = urljoin(self.base_url, path)
        return self._crawl(index_url, extension=extension, depth=max_depth)

    def _crawl(self, index_url: str, extension: str, depth: int) -> list[str]:
        logger.info("Listando %s em %s", extension, index_url)
        response = self.session.get(index_url, timeout=self.timeout)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        files: list[str] = []
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if href in ("../", "./") or href.startswith(("?", "#")):
                continue

            full_url = urljoin(index_url, href)
            if href.endswith("/"):
                if depth <= 0:
                    logger.warning("Profundidade máxima atingida, não descendo em %s", full_url)
                    continue
                files.extend(self._crawl(full_url, extension=extension, depth=depth - 1))
            elif href.lower().endswith(extension.lower()):
                files.append(full_url)

        logger.info("Encontrados %d arquivos %s em %s", len(files), extension, index_url)
        return files

    @staticmethod
    def _partition_for(filename: str) -> str:
        """Deriva a partição ano=YYYY/mes=MM a partir do nome do arquivo.

        Cai em 'unparsed' quando o nome foge do padrão esperado, em vez de falhar
        a ingestão inteira por causa de um arquivo com layout diferente.
        """
        match = _YEAR_MONTH_PATTERN.search(filename)
        if not match:
            logger.warning("Não foi possível extrair ano/mês de '%s'; usando partição 'unparsed'", filename)
            return "unparsed"
        year = match.group("year")
        month = int(match.group("month"))
        return f"{year}/{month:02d}"

    def download_file(self, file_url: str, dest_dir: Path, overwrite: bool = False) -> Path:
        """Baixa um arquivo para dest_dir/ano=YYYY/mes=MM/, em streaming. Idempotente: pula se já existir."""
        filename = file_url.rsplit("/", 1)[-1]
        partition_dir = dest_dir / self._partition_for(filename)
        partition_dir.mkdir(parents=True, exist_ok=True)
        dest_path = partition_dir / filename

        if dest_path.exists() and not overwrite:
            logger.info("Já existe, pulando: %s", dest_path)
            return dest_path

        logger.info("Baixando %s -> %s", file_url, dest_path)
        with self.session.get(file_url, timeout=self.timeout, stream=True) as response:
            response.raise_for_status()
            with dest_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

        logger.info("Baixado: %s", dest_path)
        return dest_path

    def download_all(self, path: str, dest_dir: Path, extension: str = ".csv", overwrite: bool = False) -> list[Path]:
        """Lista e baixa todos os arquivos de uma página de índice, particionados por ano/mês."""
        urls = self.list_files(path, extension=extension)
        logger.info("Iniciando download de %d arquivos para %s", len(urls), dest_dir)
        return [self.download_file(url, dest_dir, overwrite=overwrite) for url in urls]
