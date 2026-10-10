import re

from anac_lakehouse.pipelines.models.l2b import LandingToBronzeConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

# Caracteres que o Delta não aceita em nome de coluna: ' ,;{}()\n\t='
_INVALID_COLUMN_CHARS = re.compile(r"[ ,;{}()\n\t=]+")


class LandingToBronze:
    """Ingestão genérica landing -> bronze via Auto Loader (Structured Streaming + cloudFiles).

    Reutilizável entre fontes: cada fonte só passa um LandingToBronzeConfig diferente.
    Nenhuma regra de negócio é aplicada aqui -- só schema bruto (evoluído conforme chega) e
    metadados de ingestão (_source_file, _ingested_at, _row_hash), como a Bronze exige.
    """

    def __init__(self, spark: SparkSession, config: LandingToBronzeConfig):
        self.spark = spark
        self.config = config
        self.checkpoint_path = config.checkpoint_path.rstrip("/")

    def read_data(self) -> DataFrame:
        reader = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", self.config.file_format)
            .option("cloudFiles.schemaLocation", f"{self.checkpoint_path}/_schema")
            .option("cloudFiles.schemaEvolutionMode", self.config.schema_evolution_mode)
            .option("pathGlobFilter", self.config.path_glob_filter)
        )
        for key, value in self.config.reader_options.items():
            reader = reader.option(key, value)

        df = reader.load(self.config.source_path)
        df = self._sanitize_columns(df)

        df = df.select("*", "_metadata")
        data_columns = [c for c in df.columns if c != "_metadata"]
        return self._with_ingestion_metadata(df, data_columns)

    @classmethod
    def _sanitize_columns(cls, df: DataFrame) -> DataFrame:
        """Renomeia colunas com caracteres que o Delta rejeita (espaço, parênteses etc.).

        Não é transformação de negócio -- só torna o schema bruto gravável. Nomes "bonitos"
        de domínio ficam para a Silver.
        """
        for name in df.columns:
            sanitized = cls._sanitize_column_name(name)
            if sanitized != name:
                df = df.withColumnRenamed(name, sanitized)
        return df

    @staticmethod
    def _sanitize_column_name(name: str) -> str:
        if name.startswith("_"):
            return name
        return _INVALID_COLUMN_CHARS.sub("_", name.strip()).strip("_").lower()

    @staticmethod
    def _with_ingestion_metadata(df: DataFrame, data_columns: list[str]) -> DataFrame:
        row_hash = F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in data_columns]), 256)
        return (
            df.withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_row_hash", row_hash)
        )

    def write_data(self, df: DataFrame) -> StreamingQuery:
        return (
            df.writeStream.format("delta")
            .option("checkpointLocation", f"{self.checkpoint_path}/_checkpoint")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(self.config.target_table)
        )

    def run(self) -> None:
        df = self.read_data()
        query = self.write_data(df)
        query.awaitTermination()
