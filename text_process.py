import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")

with app.setup:
    import os
    from pathlib import Path

    import marimo as mo
    import orjson
    import polars as pl

    from utils import (
        get_pronouns,
        get_textmap,
        process_whitespace,
        remove_tags,
        replace_terms,
    )

    DATA_PATH = Path(os.environ["REF_DATA_PATH"])
    LANGS = os.environ["LANGS"].split(",")
    VERSION = os.environ["VERSION"]
    INTPUT_PATH = Path("staging/text0")
    OUTPUT_PATH = Path("staging/text1")


@app.cell
def _():
    DATA_PATH, LANGS, VERSION
    return


@app.cell
def _():
    with open("localization.json") as _f:
        locs = orjson.loads(_f.read())
    return (locs,)


@app.cell
def _():
    textmap = {}
    pros = {}
    for _lang in LANGS:
        textmap[_lang] = get_textmap(DATA_PATH / "TextMap", _lang)
        pros[_lang] = get_pronouns(
            DATA_PATH / "ExcelBinOutput/ManualTextMapConfigData.json",
            _lang,
            textmap[_lang],
        )
    return (pros,)


@app.cell
def _(locs, pros):
    def enhance_text(df: pl.DataFrame, lang: str):
        return df.with_columns(
            pl.col.value.pipe(process_whitespace)
            .pipe(remove_tags)
            .pipe(replace_terms, locs, pros, lang),
        ).with_columns(
            keyLower=pl.col.key.str.to_lowercase(),
            valueLower=pl.col.value.str.to_lowercase(),
            pagedLower=pl.col.paged.str.to_lowercase(),
            bookLower=pl.col.book.str.to_lowercase(),
            letterLower=pl.col.letter.str.to_lowercase(),
        )

    return (enhance_text,)


@app.function
def track_kv(df: pl.DataFrame):
    return df.sort("version", "value", "type", "key").with_columns(
        pl.col.version.first().over("key").alias("k_from"),
        pl.col.version.last().over("key").alias("k_to"),
        pl.col.version.first().over("value").alias("v_from"),
        pl.col.version.last().over("value").alias("v_to"),
        pl.col.version.first().over("key", "value").alias("kv_from"),
        pl.col.version.last().over("key", "value").alias("kv_to"),
    )


@app.cell
def _(enhance_text):
    _cols = ["value", "type", "key", "version"]
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    for _lang in LANGS:
        (
            pl.scan_parquet(str(INTPUT_PATH / f"GI_Text_{_lang}_Single_*.parquet"))
            .filter(pl.col.version <= VERSION.replace("_", "."))
            .pipe(enhance_text, _lang)
            .pipe(track_kv)
            .sort("value", "type", "key", "version")
            .sink_parquet(OUTPUT_PATH / f"GI_Text_{_lang}_{VERSION}.parquet")
        )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
