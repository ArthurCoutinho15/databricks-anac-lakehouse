from dataclasses import dataclass, field


@dataclass(frozen=True)
class LandingToBronzeConfig:
    """Configuração de uma ingestão landing -> bronze via Auto Loader para uma fonte específica."""

    source_path: str
    checkpoint_path: str
    target_table: str
    file_format: str = "csv"
    path_glob_filter: str = "*"
    # addNewColumns: schema drift não deve derrubar a Bronze inteira; falhas de schema
    # ficam para as expectations da Silver.
    schema_evolution_mode: str = "addNewColumns"
    reader_options: dict[str, str] = field(default_factory=dict)
