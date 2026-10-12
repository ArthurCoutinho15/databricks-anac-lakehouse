"""Silver do BCB: câmbio/IPCA tipados, com nome de série legível, e deduplicados.

Pendência consciente: forward-fill nos dias sem cotação (fim de semana/feriado) não é
feito aqui -- o DESAFIO pede isso na hora de juntar com a data do voo, ou seja, é
trabalho de Gold, não de Silver (aqui só limpamos/tipamos a série como ela realmente é).

`spark` é injetado pelo runtime do Lakeflow Declarative Pipelines, não precisa importar.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog", "anac")
BRONZE_TABLE = f"{CATALOG}.bronze.bcb"

_BUSINESS_KEY = ["codigo", "data"]

_VALID_SERIES_CONDITION = "codigo IS NOT NULL AND data IS NOT NULL"


def _read_and_standardize() -> DataFrame:
    df = spark.readStream.table(BRONZE_TABLE)

    df = (
        df.withColumn("data", F.to_date(F.col("data"), "dd/MM/yyyy"))
        .withColumn("valor", F.col("valor").cast("double"))
        .withColumn(
            "serie_nome",
            F.when(F.col("codigo") == 1, "dolar_venda")
            .when(F.col("codigo") == 10813, "dolar_compra")
            .when(F.col("codigo") == 433, "ipca")
            .otherwise(F.col("codigo").cast("string")),
        )
    )

    df = df.drop("_metadata", "_row_hash")

    # Dedup incremental streaming-compatível, mesmo padrão da VRA/Open-Meteo.
    df = df.withWatermark("data", "30 days").dropDuplicatesWithinWatermark(_BUSINESS_KEY)

    return df


@dp.table(
    name="bcb",
    comment=(
        "Séries do BCB (câmbio e IPCA) tipadas, com nome de série legível e deduplicadas. "
        "Sem forward-fill -- ver pendência no topo do arquivo."
    ),
)
# drop: sem codigo ou data a linha não identifica a série nem junta com nada.
@dp.expect_or_drop("tem_codigo", "codigo IS NOT NULL")
@dp.expect_or_drop("tem_data", "data IS NOT NULL")
def silver_bcb():
    return _read_and_standardize()


@dp.table(
    name="bcb_quarantine",
    comment="Registros do BCB sem codigo ou data -- quarentena em vez de descarte.",
)
def silver_bcb_quarantine():
    return _read_and_standardize().filter(f"NOT ({_VALID_SERIES_CONDITION})")
