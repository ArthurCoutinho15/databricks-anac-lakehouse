"""Silver do OurAirports: dim_aeroporto limpa, só com o snapshot mais recente por aeroporto.

Pendência consciente: timezone por aeroporto não vem nesta fonte -- precisaria de um
lookup geográfico lat/long -> timezone, fora de escopo por ora (ver anac_vra_silver.py).

`spark` é injetado pelo runtime do Lakeflow Declarative Pipelines, não precisa importar.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("catalog", "anac")
BRONZE_TABLE = f"{CATALOG}.bronze.ourairports"

_COLUMN_RENAMES = {
    "id": "id_aeroporto",
    "type": "tipo_aerodromo",
    "name": "nome",
    "latitude_deg": "latitude",
    "longitude_deg": "longitude",
    "continent": "continente",
    "iso_country": "pais_iso",
    "iso_region": "regiao_iso",
    "municipality": "municipio",
    "scheduled_service": "servico_regular",
    "icao_code": "icao",
    "iata_code": "iata",
    "gps_code": "codigo_gps",
    "local_code": "codigo_local",
}

_TYPE_CASTS = {
    "latitude": "double",
    "longitude": "double",
    "elevation_ft": "int",
}

_VALID_AIRPORT_CONDITION = "icao IS NOT NULL"


def _read_data_and_standardize() -> DataFrame:
    # Dimensão pequena, atualizada diariamente -- materializa como batch completo (não
    # streaming) porque precisamos ficar só com o snapshot mais recente por aeroporto, e
    # isso exige row_number()/Window, que não funciona em DataFrame de streaming.
    df = spark.read.table(BRONZE_TABLE)

    df = df.withColumnsRenamed(_COLUMN_RENAMES)
    df = df.drop("_metadata", "_row_hash", "home_link", "wikipedia_link", "keywords")

    for column, target_type in _TYPE_CASTS.items():
        df = df.withColumn(column, F.col(column).cast(target_type))

    df = df.withColumn("servico_regular", F.col("servico_regular") == F.lit("yes"))


    window = Window.partitionBy("id_aeroporto").orderBy(F.col("dt").desc())
    df = df.withColumn("_dedup_rank", F.row_number().over(window)).filter(F.col("_dedup_rank") == 1)

    return df.drop("_dedup_rank")


@dp.table(
    name="aeroporto",
    comment="Dimensão de aeroportos (OurAirports), só o snapshot mais recente por aeroporto.",
)
# drop: sem icao não dá pra juntar com a VRA (que só usa código ICAO) -- aeroporto
# inútil pra esse projeto sem isso.
@dp.expect_or_drop("tem_icao", _VALID_AIRPORT_CONDITION)
# warn: aeródromo fechado é dado válido e precisa continuar na dimensão (voos antigos da
# VRA podem referenciar um aeroporto que já fechou) -- é só sinal de monitoramento.
@dp.expect("aerodromo_fechado_sinalizado", "tipo_aerodromo != 'closed'")
def silver_aeroporto():
    return _read_data_and_standardize()


@dp.table(
    name="aeroporto_quarantine",
    comment=(
        "Aeroportos sem código ICAO -- quarentena em vez de descarte, "
        "serve de base pro relatório de não encontrados do DESAFIO."
    ),
)
def silver_aeroporto_quarantine():
    return _read_data_and_standardize().filter(f"NOT ({_VALID_AIRPORT_CONDITION})")
