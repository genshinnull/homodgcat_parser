import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import os
    from pathlib import Path

    import marimo as mo
    import polars as pl

    LANGS = os.environ["LANGS"].split(",")
    VERSION = os.environ["VERSION"]
    INTPUT_PATH = Path("staging/text0")
    OUTPUT_PATH = Path("staging/text1")


@app.cell
def _():
    LANGS, VERSION
    return


@app.cell
def _():
    text_data = {}
    for _lang in LANGS:
        text_data[_lang] = (
            pl.read_parquet(INTPUT_PATH / f"GI_Text_{_lang}_{VERSION}.parquet")
            .sort("version", "value", "type", "key", "paged", "book", "letter")
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
    return (text_data,)


@app.cell
def _(text_data):
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    for _lang in LANGS:
        text_data[_lang].sort(
            "value", "type", "key", "version"
        ).write_parquet(OUTPUT_PATH / f"GI_Text_{_lang}_{VERSION}.parquet")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
