import logging
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class AnpQavClient:
    """Client para o preço médio do QAV por município no Anuário Estatístico da ANP.

    URL fixada na edição 2025 (tabela 3.26 -- "preço médio do QAV ao consumidor" por
    município) em vez de descoberta automática por edição: o layout muda de um jeito
    sério entre edições -- confirmado que até o *número* da tabela muda de conteúdo ano
    a ano (ex.: a edição 2026 não tem mais essa tabela nessa posição, e um scanner por
    conteúdo de header pegou a tabela errada -- "preço de produtores/importadores" em
    vez de "preço ao consumidor por município" -- porque ambas mencionam QAV). Ver ADR
    em docs/decisoes.md. Reavaliar manualmente quando uma edição mais nova tiver os
    mesmos anos cobertos.
    """

    DEFAULT_URL = (
        "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/"
        "anuario-estatistico-2025/secao-3/anuario-2025-dados_abertos-tabela3-26.csv"
    )

    def __init__(self, url: str = DEFAULT_URL, timeout: int = 30, max_retries: int = 5, backoff_factor: float = 1.5):
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def download_qav_table(self, dest_dir: Path, overwrite: bool = False) -> Path:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / "preco_medio_qav.csv"

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
