from pathlib import Path


SCRIPT = Path("src/rrsmr_ads/objective3_smr_integration/owner3_data_integration.py")


def test_owner3_does_not_force_fastparquet_engine():
    text = SCRIPT.read_text(encoding="utf-8")

    assert "engine=\"fastparquet\"" not in text
    assert "from fastparquet" not in text
    assert "fastparquet.write" not in text


def test_owner3_partitioned_writer_uses_pandas_partition_cols():
    text = SCRIPT.read_text(encoding="utf-8")

    assert "partition_cols=partition_cols" in text
    assert "remove_path(path)" in text
