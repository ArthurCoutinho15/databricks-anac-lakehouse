import logging
from datetime import date, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Desde mar/2025 a API SGS limita consultas por período a 10 anos por requisição.
_MAX_YEARS_PER_REQUEST = 10


class BcbClient:
    """Client para a API SGS do Banco Central, com retry/backoff e particionamento por 10 anos."""

    def __init__(
        self,
        base_url: str = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados",
        timeout: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 2.0,
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def fetch_series(self, codigo: int, start_date: date, end_date: date) -> list[dict]:
        """Busca a série entre start_date e end_date, particionando em blocos de até 10 anos."""
        rows = []
        for chunk_start, chunk_end in self._date_chunks(start_date, end_date):
            rows.extend(self._fetch_chunk(codigo, chunk_start, chunk_end))
        return rows

    def _fetch_chunk(self, codigo: int, start_date: date, end_date: date) -> list[dict]:
        url = self.base_url.format(codigo=codigo)
        params = {
            "formato": "json",
            "dataInicial": start_date.strftime("%d/%m/%Y"),
            "dataFinal": end_date.strftime("%d/%m/%Y"),
        }
        logger.info("Buscando série %d: %s a %s", codigo, start_date, end_date)
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()

        rows = [{"codigo": codigo, "data": item["data"], "valor": item["valor"]} for item in payload]
        logger.info("Recebidos %d registros da série %d", len(rows), codigo)
        return rows

    @staticmethod
    def _date_chunks(start_date: date, end_date: date):
        """Gera blocos [início, fim] de no máximo 10 anos corridos, inclusive."""
        chunk_start = start_date
        while chunk_start <= end_date:
            try:
                next_decade = chunk_start.replace(year=chunk_start.year + _MAX_YEARS_PER_REQUEST)
            except ValueError:
                # 29/fev cai num ano não bissexto 10 anos depois
                next_decade = chunk_start.replace(year=chunk_start.year + _MAX_YEARS_PER_REQUEST, day=28)

            chunk_end = min(next_decade - timedelta(days=1), end_date)
            yield chunk_start, chunk_end
            chunk_start = chunk_end + timedelta(days=1)
