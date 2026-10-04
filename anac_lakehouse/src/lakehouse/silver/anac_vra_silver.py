"""Silver da VRA: tipagem, dedup e cálculo de atraso.

Pendências conscientes (dependem de dados que o projeto ainda não ingeriu):
  - Conversão pra UTC por timezone do aeroporto -- precisa de uma dim_aeroporto com
    timezone (OurAirports + lookup geográfico). Os horários aqui continuam em hora
    local do aeroporto. atraso_partida_min/atraso_chegada_min continuam corretos mesmo
    assim, pois são diferença local-local (mesmo fuso nas duas pontas).
  - De-para de código de justificativa -- precisa da tabela de referência da ANAC,
    ainda não ingerida. codigo_justificativa fica como veio da Bronze.

`spark` é injetado pelo runtime do Lakeflow Declarative Pipelines, não precisa importar.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "anac")
BRONZE_TABLE = f"{CATALOG}.bronze.vra"


_COLUMN_RENAMES = {
    "icao_empresa_aérea": "icao_companhia",
    "número_voo": "numero_voo",
    "código_autorização_di": "codigo_autorizacao_di",
    "código_tipo_linha": "codigo_tipo_linha",
    "icao_aeródromo_origem": "icao_aerodromo_origem",
    "icao_aeródromo_destino": "icao_aerodromo_destino",
    "situação_voo": "situacao_voo",
    "código_justificativa": "codigo_justificativa",
}

_TIMESTAMP_COLUMNS = ["partida_prevista", "partida_real", "chegada_prevista", "chegada_real"]
_REAL_TIME_COLUMNS = ["partida_real", "chegada_real"]
_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

# Chave de negócio de um voo: mesma companhia + número + horário previsto de partida.
_BUSINESS_KEY = ["icao_companhia", "numero_voo", "partida_prevista"]

_VALID_FLIGHT_CONDITION = (
    "numero_voo IS NOT NULL AND icao_companhia IS NOT NULL "
    "AND icao_aerodromo_origem IS NOT NULL AND partida_prevista IS NOT NULL"
)

_TYPE_CASTS = {
    "numero_voo": "int",
    "codigo_autorizacao_di": "int",
}


def _read_and_standardize():
    df: DataFrame = spark.readStream.table(BRONZE_TABLE)

    for old_name, new_name in _COLUMN_RENAMES.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    for column in _REAL_TIME_COLUMNS:
        df = df.withColumn(column, F.when(F.col(column) == "null", None).otherwise(F.col(column)))

    for column in _TIMESTAMP_COLUMNS:
        df = df.withColumn(column, F.to_timestamp(F.col(column), _TIMESTAMP_FORMAT))

    for column, target_type in _TYPE_CASTS.items():
        df = df.withColumn(column, F.col(column).cast(target_type))

    df = (
        df.withColumn(
            "atraso_partida_min",
            (F.col("partida_real").cast("long") - F.col("partida_prevista").cast("long")) / 60,
        )
        .withColumn(
            "atraso_chegada_min",
            (F.col("chegada_real").cast("long") - F.col("chegada_prevista").cast("long")) / 60,
        )
        .withColumn("cancelado", F.col("situacao_voo") == F.lit("CANCELADO"))
        .withColumn(
            "codigo_justificativa",
            F.when(F.col("codigo_justificativa") == "N/A", None).otherwise(F.col("codigo_justificativa")),
        )
    )

    # Dedup incremental streaming-compatível: row_number()/Window não funciona em
    # streaming (exigiria um estado ilimitado); dropDuplicatesWithinWatermark é o
    # equivalente suportado, usando partida_prevista como tempo de evento.
    df = df.withWatermark("partida_prevista", "3 days").dropDuplicatesWithinWatermark(_BUSINESS_KEY)

    return df.drop("_metadata", "_row_hash")


@dp.table(
    name="vra",
    comment=(
        "Voos VRA tipados, deduplicados por voo e com atraso calculado. Horários ainda em "
        "hora local do aeroporto -- ver pendência de timezone no topo do arquivo."
    ),
)
@dp.expect_or_drop("tem_numero_voo", "numero_voo IS NOT NULL")
@dp.expect_or_drop("tem_companhia", "icao_companhia IS NOT NULL")
@dp.expect_or_drop("tem_aerodromo_origem", "icao_aerodromo_origem IS NOT NULL")
@dp.expect_or_drop("tem_partida_prevista", "partida_prevista IS NOT NULL")
@dp.expect("atraso_partida_plausivel", "atraso_partida_min IS NULL OR atraso_partida_min BETWEEN -120 AND 1440")
def silver_vra():
    return _read_and_standardize()


@dp.table(
    name="vra_quarantine",
    comment="Voos VRA que falharam nas validações mínimas da tabela 'vra' -- quarentena em vez de descarte.",
)
def silver_vra_quarantine():
    return _read_and_standardize().filter(f"NOT ({_VALID_FLIGHT_CONDITION})")
