"""Silver union: csvs_to_parquet must combine N part files (full-mode regression).

Bug real (2026-10-10, run full): com 10 parts (empresas) o SQL gerado era
``read_parquet('a','b',...,'j')`` — overload inexistente (só VARCHAR único ou
VARCHAR[]) — e o DuckDB falhava com BinderException. Smoke (1 parte) nunca
exercitava o caminho. O fix usa sintaxe de lista: ``read_parquet(['a','b'])``.
"""

import sys
from pathlib import Path


def _write_csv(path: Path, rows: list[tuple[str, str]]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for codigo, descricao in rows:
            handle.write(f"{codigo};{descricao}\n")


def test_csvs_to_parquet_multi_part_union(tmp_path):
    sys.path.insert(0, "include")
    from kolmeia.silver import COLUMNS, csvs_to_parquet

    csvs = []
    for idx, rows in enumerate([[("1111111", "A UM"), ("2222222", "A DOIS")], [("3333333", "A TRES")]]):
        csv = tmp_path / f"in{idx}.csv"
        _write_csv(csv, rows)
        csvs.append(csv)

    output = tmp_path / "combined.parquet"
    total = csvs_to_parquet(csvs, output, COLUMNS["cnaes"])

    assert total == 3
    assert output.exists()
    import polars as pl

    got = pl.scan_parquet(str(output)).select(pl.len()).collect().item()
    assert got == 3


def test_csvs_to_parquet_single_part_still_works(tmp_path):
    """Smoke path (1 parte) continua funcionando após o fix."""
    sys.path.insert(0, "include")
    from kolmeia.silver import COLUMNS, csvs_to_parquet

    csv = tmp_path / "one.csv"
    _write_csv(csv, [("1111111", "A UM")])
    output = tmp_path / "single.parquet"
    assert csvs_to_parquet([csv], output, COLUMNS["cnaes"]) == 1
