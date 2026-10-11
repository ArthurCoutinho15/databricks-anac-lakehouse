"""Silver do Open-Meteo: fato_clima_aeroporto_hora tipado, deduplicado e em português.

Diferente da VRA, não tem pendência de timezone aqui -- a ingestão já pede a API com
timezone=UTC, então data_hora já nasce em UTC.

Limitação conhecida da fonte (não é bug): `visibility` não existe na Historical Weather
API (só na API de previsão), então sempre chega NULL -- dropamos em vez de manter uma
coluna sempre vazia.

`spark` é injetado pelo runtime do Lakeflow Declarative Pipelines, não precisa importar.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "anac")
BRONZE_TABLE = f"{CATALOG}.bronze.open_meteo"

_COLUMN_RENAMES = {
    "time": "data_hora",
    "temperature_2m": "temperatura_c",
    "precipitation": "precipitacao_mm",
    "wind_speed_10m": "vento_kmh",
    "cloud_cover": "cobertura_nuvens_pct",
}

_TYPE_CASTS = {
    "data_hora": "timestamp",
    "temperatura_c": "double",
    "precipitacao_mm": "double",
    "vento_kmh": "double",
    "cobertura_nuvens_pct": "double",
}

_BUSINESS_KEY = ["icao", "data_hora"]

_VALID_WEATHER_CONDITION = "icao IS NOT NULL AND data_hora IS NOT NULL"


def _read_and_standardize() -> DataFrame:
    df = spark.readStream.table(BRONZE_TABLE)

    df = df.withColumnsRenamed(_COLUMN_RENAMES)
    df = df.drop("_metadata", "_row_hash", "_rescued_data", "visibility")

    for column, target_type in _TYPE_CASTS.items():
        df = df.withColumn(column, F.col(column).cast(target_type))

    # Dedup incremental streaming-compatível, mesmo motivo da VRA: row_number()/Window
    # não funciona em streaming (exigiria estado ilimitado).
    df = df.withWatermark("data_hora", "3 days").dropDuplicatesWithinWatermark(_BUSINESS_KEY)

    return df


@dp.table(
    name="clima_aeroporto_hora",
    comment="Clima horário por aeroporto (Open-Meteo), tipado e deduplicado. data_hora em UTC.",
)
# drop: sem icao ou data_hora a linha não junta com nada -- inútil pra qualquer análise.
@dp.expect_or_drop("tem_icao", "icao IS NOT NULL")
@dp.expect_or_drop("tem_data_hora", "data_hora IS NOT NULL")
def silver_clima_aeroporto_hora():
    return _read_and_standardize()


@dp.table(
    name="clima_aeroporto_hora_quarantine",
    comment="Registros de clima sem icao ou data_hora -- quarentena em vez de descarte.",
)
def silver_clima_aeroporto_hora_quarantine():
    return _read_and_standardize().filter(f"NOT ({_VALID_WEATHER_CONDITION})")
