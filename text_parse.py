import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    import os
    import re
    from pathlib import Path

    import orjson
    import polars as pl
    import pysubs2
    from git import Repo

    DATA_PATH = Path(os.environ["DATA_PATH"])
    CBT3_DATA_PATH = Path(os.environ["CBT3_DATA_PATH"])
    LANGS = os.environ["LANGS"].split(",")
    VERSION = os.environ["VERSION"]


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Text Parser
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Setup
    """)
    return


@app.cell
def _():
    DATA_PATH, CBT3_DATA_PATH, LANGS, VERSION
    return


@app.cell
def _():
    with open("text_versions.json") as _f:
        versions = orjson.loads(_f.read())
    len(versions)
    return (versions,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## TextMap, Readable, Subtitle
    """)
    return


@app.function
def extract_textmap(ver: str, lang: str, dir: Path) -> pl.DataFrame:
    results = []
    files = list(dir.glob("*.json"))
    for file in files:
        if re.match(rf"(Text{lang}|TextMap(_Medium)?{lang}(_\d)?)", file.stem):
            with open(file) as f:
                data = orjson.loads(f.read())
            results.append(
                pl.DataFrame({"key": data.keys(), "value": data.values()})
                .filter(pl.col.value != "")
                .select(
                    pl.lit(ver).alias("version"),
                    pl.lit("TextMap").alias("type"),
                    pl.col.key,
                    pl.col.value,
                )
            )
    return pl.concat(results) if results else pl.DataFrame()


@app.function
def extract_readable(ver: str, lang: str, dir: Path) -> pl.DataFrame:
    data = []
    for file in dir.glob("*.txt"):
        with open(file, errors="replace") as f:
            data.append(
                {
                    "key": file.stem.split(f"_{lang}")[0],
                    "value": f.read().strip(),
                }
            )
    return (
        pl.DataFrame(data).select(
            pl.lit(ver).alias("version"),
            pl.lit("Readable").alias("type"),
            pl.col.key,
            pl.col.value,
        )
        if data
        else pl.DataFrame()
    )


@app.function
def extract_subtitle(ver: str, lang: str, dir: Path) -> pl.DataFrame:
    data = []
    files = []
    files.extend(dir.glob("*.txt"))
    files.extend(dir.glob("*.srt"))
    for file in files:
        try:
            sub = pysubs2.load(file)
            sub_str = "\n".join([line.plaintext for line in sub]).strip()
        except pysubs2.exceptions.FormatAutodetectionError:
            sub_str = ""
        data.append(
            {
                "key": file.stem.split(f"_{lang}")[0],
                "value": sub_str,
            }
        )
    return (
        pl.DataFrame(data).select(
            pl.lit(ver).alias("version"),
            pl.lit("Subtitle").alias("type"),
            pl.col.key,
            pl.col.value,
        )
        if data
        else pl.DataFrame()
    )


@app.cell
def _(versions):
    text_data = {lang: [] for lang in LANGS}
    with mo.status.progress_bar(
        total=len(versions),
        remove_on_exit=True,
        title="Extracting...",
    ) as _bar:
        for _ver in versions:
            _bar.subtitle = f"Working on Version {_ver['ver']}"
            match _ver["type"]:
                case "REL":
                    _path = DATA_PATH
                case "CBT3":
                    _path = CBT3_DATA_PATH
            if _ver.get("hash"):
                _repo = Repo(_path)
                assert not _repo.is_dirty()
                for _dir in [
                    Path("TextMap"),
                    Path("Readable"),
                    Path("Subtitle"),
                ]:
                    _repo.git.restore("--source", _ver["hash"], _dir)
            for _lang in LANGS:
                for _data_df in [
                    extract_textmap(_ver["ver"], _lang, _path / "TextMap"),
                    extract_readable(
                        _ver["ver"], _lang, _path / "Readable" / _lang
                    ),
                    extract_subtitle(
                        _ver["ver"], _lang, _path / "Subtitle" / _lang
                    ),
                ]:
                    if not _data_df.is_empty():
                        text_data[_lang].append(_data_df)
            if _ver.get("hash"):
                _repo.git.clean("-fd")
                _repo.git.reset("HEAD", "--hard")
            _bar.update()
    text_data = {lang: pl.concat(data) for lang, data in text_data.items()}
    {lang: len(data) for lang, data in text_data.items()}
    return (text_data,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## LocalizationExcelConfigData
    """)
    return


@app.cell
def _():
    localization_df = (
        pl.read_json(DATA_PATH / "ExcelBinOutput/LocalizationExcelConfigData.json")
        .filter(pl.col.assetType == "LOC_TEXT")
        .select(
            pl.col.id,
            pl.col.enPath.str.split("/")
            .list.last()
            .str.split("_EN")
            .list.first()
            .alias("key"),
        )
    )
    localization_df
    return (localization_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## DocumentExcelConfigData
    """)
    return


@app.cell
def _():
    document_df = (
        (
            pl.read_json(
                DATA_PATH / "ExcelBinOutput/DocumentExcelConfigData.json",
                schema={
                    "documentType": pl.String,
                    "questIDList": pl.List(pl.Int64),
                    "titleTextMapHash": pl.String,
                },
            )
            .rename({"questIDList": "id"})
            .select(
                pl.col.documentType,
                pl.col.id,
                pl.col.titleTextMapHash,
            )
            .filter(~pl.col.documentType.is_in(["DynamicBook", "Video"]))
        )
        .explode("id")
        .pivot(
            "documentType",
            index="id",
            values="titleTextMapHash",
            aggregate_function="first",
        )
    )
    document_df
    return (document_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Transformation
    """)
    return


@app.cell
def _(document_df, localization_df, text_data):
    text_data_transformed = {}
    for _lang in LANGS:
        _tm_df = (
            text_data[_lang]
            .filter(
                pl.col.type == "TextMap", pl.col.version == pl.col.version.max()
            )
            .select("key", "value")
        )
        _readable_df = document_df.join(
            localization_df, on="id", how="inner", validate="1:1"
        ).select(
            pl.col.key,
            pl.col.Paged.replace_strict(
                _tm_df.get_column("key"),
                _tm_df.get_column("value"),
                default=None,
            ),
            pl.col.Book.replace_strict(
                _tm_df.get_column("key"),
                _tm_df.get_column("value"),
                default=None,
            ),
            pl.col.Letter.replace_strict(
                _tm_df.get_column("key"),
                _tm_df.get_column("value"),
                default=None,
            ),
        )
        text_data_transformed[_lang] = (
            text_data[_lang]
            .join(_readable_df, on="key", how="left")
            .sort("version")
            .with_columns(
                pl.col.value.str.replace_all(r"\\n", "\n").str.strip_chars(),
            )
            .with_columns(
                pl.col.version.first().over("key").alias("k_from"),
                pl.col.version.last().over("key").alias("k_to"),
                pl.col.version.first().over("value").alias("v_from"),
                pl.col.version.last().over("value").alias("v_to"),
                pl.col.version.first().over("key", "value").alias("kv_from"),
                pl.col.version.last().over("key", "value").alias("kv_to"),
            )
        )
    {lang: len(data) for lang, data in text_data.items()}
    return (text_data_transformed,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Output
    """)
    return


@app.cell
def _(text_data_transformed):
    for _lang in LANGS:
        text_data_transformed[_lang].filter(
            pl.col.version == pl.col.version.max()
        ).select(
            "type",
            "key",
            "value",
            "Paged",
            "Book",
            "Letter",
            "k_from",
            "v_from",
            "kv_from",
        ).sort("value", "type", "key").write_parquet(
            f"output/GI_Text_{_lang}_{VERSION}.parquet"
        )
        text_data_transformed[_lang].sort(
            "value", "type", "key", "version"
        ).write_parquet(f"output/GI_Text_{_lang}_{VERSION}_Full.parquet")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
