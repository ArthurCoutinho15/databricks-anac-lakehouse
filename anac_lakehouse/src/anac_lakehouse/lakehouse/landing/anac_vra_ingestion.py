import logging
import sys
from pathlib import Path

from anac_lakehouse.clients.anac_vra_client import AnacVraBaseClient

LANDING_SCHEMA = "landing"
LANDING_VOLUME = "landing"

# Caminho real no índice do ANAC: Voos e operações aéreas/Voo Regular Ativo (VRA)/{ano}/{mês} - {NomeMês}/
VRA_INDEX_PATH = "Voos e operações aéreas/Voo Regular Ativo (VRA)/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def main():
    catalog = sys.argv[1]  # vem da variável de bundle ${var.catalog}
    # Subpasta dentro do VRA a baixar; default = um único ano, para não puxar o histórico
    # inteiro (2000+) de primeira. Passe algo como "2023/01 - Janeiro/" pra testar com 1 mês,
    # ou "" pra baixar tudo quando já estiver validado.
    vra_subpath = sys.argv[2] if len(sys.argv) > 2 else "2024/"

    dest_dir = Path(f"/Volumes/{catalog}/{LANDING_SCHEMA}/{LANDING_VOLUME}/vra")
    client = AnacVraBaseClient()
    client.download_all(path=VRA_INDEX_PATH + vra_subpath, dest_dir=dest_dir)


if __name__ == "__main__":
    main()
