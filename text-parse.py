import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import os
    from pathlib import Path

    import marimo as mo
    import polars as pl
    from git import Repo

    DATA_PATH = Path(os.environ["DATA_PATH"])
    CBT3_DATA_PATH = Path(os.environ["CBT3_DATA_PATH"])
    LANGS = os.environ["LANGS"].split(",")


@app.cell
def _():
    DATA_PATH, CBT3_DATA_PATH, LANGS
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
