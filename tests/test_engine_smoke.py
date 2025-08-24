from datapulse import engine

def test_catalog_roundtrip(tmp_path, monkeypatch):
    # isolate catalog
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data.csv").write_text("a,b\n1,2\n", encoding="utf-8")

    engine.add_dataset("t", str(tmp_path / "data.csv"))
    rows = engine.list_datasets()
    assert rows and rows[0]["name"] == "t"

    df = engine.load_df("t", limit=1)
    assert list(df.columns) == ["a", "b"]
    out = engine.run_sql("SELECT COUNT(*) AS n FROM t")
    assert int(out.iloc[0]["n"]) == 1
