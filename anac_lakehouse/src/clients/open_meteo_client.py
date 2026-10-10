import logging
from datetime import date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_HOURLY_VARS = ["temperature_2m", "precipitation", "wind_speed_10m", "cloud_cover", "visibility"]


class OpenMeteoClient:
    """Client para a Historical Weather API do Open-Meteo, com retry/backoff."""

    def __init__(
        self,
        base_url: str = "https://archive-api.open-meteo.com/v1/archive",
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

    def fetch_hourly_weather(
        self, icao: str, latitude: float, longitude: float, start_date: date, end_date: date
    ) -> list[dict]:
        """Busca clima horário entre start_date e end_date (inclusive), já 'zipado' em linhas."""
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": ",".join(_HOURLY_VARS),
            "timezone": "UTC",
        }
        logger.info("Buscando clima de %s: %s a %s", icao, start_date, end_date)
        response = self.session.get(self.base_url, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()

        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])

        rows = []
        for i, timestamp in enumerate(times):
            row = {"icao": icao, "time": timestamp}
            for var in _HOURLY_VARS:
                values = hourly.get(var, [])
                row[var] = values[i] if i < len(values) else None
            rows.append(row)

        logger.info("Recebidas %d horas de clima para %s", len(rows), icao)
        return rows
