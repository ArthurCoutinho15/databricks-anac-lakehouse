import json
import logging
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from anac_lakehouse.clients.open_meteo_client import OpenMeteoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType

LANDING_SCHEMA = "landing"
LANDING_VOLUME = "landing"
WATERMARK_TABLE_SUFFIX = "system.watermark_clima"

# Fase piloto: começar pequeno, com os aeroportos que já aparecem no mês de VRA testado
# (2023-01). Ampliar depois lendo os ICAOs distintos de origem/destino de silver.vra.
DEFAULT_ICAOS = ["SBFZ", "SBRF", "SBGR", "SBCY", "SNIG"]

# A API archive do Open-Meteo atrasa alguns dias pra consolidar dados recentes --
# evita pedir os últimos dias "quentes", que podem vir incompletos.
_ARCHIVE_LAG_DAYS = 7
_DEFAULT_START_DATE = date(2023, 1, 1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

_WATERMARK_SCHEMA = StructType(
    [
        StructField("icao", StringType(), nullable=False),
        StructField("ultima_data_buscada", DateType(), nullable=False),
    ]
)


def _watermark_table(catalog: str) -> str:
    return f"{catalog}.{WATERMARK_TABLE_SUFFIX}"


def _ensure_watermark_table(spark: SparkSession, catalog: str) -> None:
    table = _watermark_table(catalog)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} (icao STRING NOT NULL, ultima_data_buscada DATE NOT NULL) USING DELTA"
    )


def _last_fetched_date(spark: SparkSession, catalog: str, icao: str) -> date | None:
    table = _watermark_table(catalog)
    row = spark.sql(f"SELECT ultima_data_buscada FROM {table} WHERE icao = '{icao}'").first()
    return row["ultima_data_buscada"] if row else None


def _update_watermark(spark: SparkSession, catalog: str, icao: str, last_date: date) -> None:
    table = _watermark_table(catalog)
    update_df = spark.createDataFrame([(icao, last_date)], schema=_WATERMARK_SCHEMA)
    update_df.createOrReplaceTempView("_watermark_update")
    spark.sql(f"""
        MERGE INTO {table} AS target
        USING _watermark_update AS source
        ON target.icao = source.icao
        WHEN MATCHED THEN UPDATE SET target.ultima_data_buscada = source.ultima_data_buscada
        WHEN NOT MATCHED THEN INSERT (icao, ultima_data_buscada) VALUES (source.icao, source.ultima_data_buscada)
    """)


def _airport_coordinates(spark: SparkSession, catalog: str, icaos: list[str]) -> dict[str, tuple[float, float]]:
    df = (
        spark.table(f"{catalog}.silver.aeroporto")
        .filter(F.col("icao").isin(icaos))
        .select("icao", "latitude", "longitude")
    )
    return {row["icao"]: (row["latitude"], row["longitude"]) for row in df.collect()}


def open_meteo_ingestion(catalog: str, icaos: list[str]) -> None:
    spark = SparkSession.builder.getOrCreate()
    _ensure_watermark_table(spark, catalog)

    coordinates = _airport_coordinates(spark, catalog, icaos)
    missing = [icao for icao in icaos if icao not in coordinates]
    if missing:
        logger.warning("Sem coordenadas em silver.aeroporto para: %s -- pulando", missing)

    client = OpenMeteoClient()
    dest_dir = Path(f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}/open_meteo")
    max_end_date = datetime.now(tz=UTC).date() - timedelta(days=_ARCHIVE_LAG_DAYS)

    for icao in icaos:
        if icao not in coordinates:
            continue

        latitude, longitude = coordinates[icao]
        last_date = _last_fetched_date(spark, catalog, icao)
        start_date = (last_date + timedelta(days=1)) if last_date else _DEFAULT_START_DATE

        if start_date > max_end_date:
            logger.info("%s já está atualizado até %s, nada a buscar", icao, last_date)
            continue

        rows = client.fetch_hourly_weather(icao, latitude, longitude, start_date, max_end_date)
        if not rows:
            continue

        partition_dir = dest_dir / f"icao={icao}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        dest_path = partition_dir / f"{start_date.isoformat()}_{max_end_date.isoformat()}.json"
        # JSON Lines (um objeto por linha) -- é o que o Auto Loader espera por padrão
        # pro formato "json"; um array único em uma linha não funciona sem multiLine=true.
        dest_path.write_text("\n".join(json.dumps(row) for row in rows))
        logger.info("Gravado: %s", dest_path)

        _update_watermark(spark, catalog, icao, max_end_date)


def main():
    # Databricks chama main() sem argumentos quando roda via python_wheel_task;
    # os parâmetros do job chegam por sys.argv aqui dentro.
    catalog = sys.argv[1]
    icaos = sys.argv[2].split(",") if len(sys.argv) > 2 else DEFAULT_ICAOS

    open_meteo_ingestion(catalog, icaos)


if __name__ == "__main__":
    main()
