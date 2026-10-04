import logging
import sys

from anac_lakehouse.pipelines.landing_to_bronze import LandingToBronze
from anac_lakehouse.pipelines.models.l2b import LandingToBronzeConfig
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def bronze_anac_vra(spark: SparkSession, catalog: str):
    l2b = LandingToBronze(
        spark,
        config=LandingToBronzeConfig(
            source_path=f"/Volumes/{catalog}/landing/landing/vra",
            checkpoint_path=f"/Volumes/{catalog}/system/checkpoints/vra",
            target_table=f"{catalog}.bronze.vra",
            schema_evolution_mode="addNewColumns",
            reader_options={
                "header": "true",
                "sep": ";",
                "skipRows": 1,
                "encoding": "UTF-8",
                "quote": '"',
            },
        ),
    )

    l2b.run()


def main():
    # Databricks chama main() sem argumentos quando roda via python_wheel_task;
    # os parâmetros do job chegam por sys.argv aqui dentro.
    catalog = sys.argv[1]
    spark = SparkSession.builder.getOrCreate()

    bronze_anac_vra(spark, catalog)


if __name__ == "__main__":
    main()
