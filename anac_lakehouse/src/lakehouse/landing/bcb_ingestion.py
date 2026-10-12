import json
import logging
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from anac_lakehouse.clients.bcb_client import BcbClient
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StructField, StructType

LANDING_SCHEMA = "landing"
LANDING_VOLUME = "landing"
WATERMARK_TABLE_SUFFIX = "system.watermark_bcb"

# Séries sugeridas pelo DESAFIO: dólar venda (diária), dólar compra (diária), IPCA (mensal).
DEFAULT_SERIES = [1, 10813, 433]

_DEFAULT_START_DATE = date(2023, 1, 1)
# A cotação/índice do dia ainda não está publicado quando o job roda; evita pedir "hoje".
_LAG_DAYS = 1

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

_WATERMARK_SCHEMA = StructType(
    [
        StructField("codigo", IntegerType(), nullable=False),
        StructField("ultima_data_buscada", DateType(), nullable=False),
    ]
)


def _watermark_table(catalog: str) -> str:
    return f"{catalog}.{WATERMARK_TABLE_SUFFIX}"


def _ensure_watermark_table(spark: SparkSession, catalog: str) -> None:
    table = _watermark_table(catalog)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} (codigo INT NOT NULL, ultima_data_buscada DATE NOT NULL) USING DELTA"
    )


def _last_fetched_date(spark: SparkSession, catalog: str, codigo: int) -> date | None:
    table = _watermark_table(catalog)
    row = spark.sql(f"SELECT ultima_data_buscada FROM {table} WHERE codigo = {codigo}").first()
    return row["ultima_data_buscada"] if row else None


def _update_watermark(spark: SparkSession, catalog: str, codigo: int, last_date: date) -> None:
    table = _watermark_table(catalog)
    update_df = spark.createDataFrame([(codigo, last_date)], schema=_WATERMARK_SCHEMA)
    update_df.createOrReplaceTempView("_watermark_update")
    spark.sql(f"""
        MERGE INTO {table} AS target
        USING _watermark_update AS source
        ON target.codigo = source.codigo
        WHEN MATCHED THEN UPDATE SET target.ultima_data_buscada = source.ultima_data_buscada
        WHEN NOT MATCHED THEN INSERT (codigo, ultima_data_buscada) VALUES (source.codigo, source.ultima_data_buscada)
    """)


def bcb_ingestion(catalog: str, series: list[int]) -> None:
    spark = SparkSession.builder.getOrCreate()
    _ensure_watermark_table(spark, catalog)

    client = BcbClient()
    dest_dir = Path(f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}/bcb")
    max_end_date = datetime.now(tz=UTC).date() - timedelta(days=_LAG_DAYS)

    for codigo in series:
        last_date = _last_fetched_date(spark, catalog, codigo)
        start_date = (last_date + timedelta(days=1)) if last_date else _DEFAULT_START_DATE

        if start_date > max_end_date:
            logger.info("Série %d já está atualizada até %s, nada a buscar", codigo, last_date)
            continue

        rows = client.fetch_series(codigo, start_date, max_end_date)
        if not rows:
            continue

        partition_dir = dest_dir / f"codigo={codigo}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        dest_path = partition_dir / f"{start_date.isoformat()}_{max_end_date.isoformat()}.json"
        # JSON Lines, mesmo formato usado na ingestão do Open-Meteo.
        dest_path.write_text("\n".join(json.dumps(row) for row in rows))
        logger.info("Gravado: %s", dest_path)

        _update_watermark(spark, catalog, codigo, max_end_date)


def main():
    # Databricks chama main() sem argumentos quando roda via python_wheel_task;
    # os parâmetros do job chegam por sys.argv aqui dentro.
    catalog = sys.argv[1]
    series = [int(c) for c in sys.argv[2].split(",")] if len(sys.argv) > 2 else DEFAULT_SERIES

    bcb_ingestion(catalog, series)


if __name__ == "__main__":
    main()
