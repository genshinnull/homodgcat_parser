import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")

with app.setup:
    import os
    import re
    from pathlib import Path

    import marimo as mo
    import orjson
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
    with open("text-versions.json") as _f:
        versions = orjson.loads(_f.read())
    len(versions)
    return (versions,)


@app.function
def extract_textmap(ver: str, lang: str, dir: Path) -> list[pl.DataFrame]:
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
    return results


@app.function
def extract_readable(ver: str, lang: str, dir: Path) -> pl.DataFrame:
    data = []
    for file in dir.glob("*.txt"):
        with open(file) as f:
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


@app.cell
def _(versions):
    text_data = {lang: [] for lang in LANGS}
    with mo.status.progress_bar(
        total=len(versions),
        remove_on_exit=True,
        title="Extracting...",
    ) as bar:
        for _ver in versions:
            bar.subtitle = f"Working on Version {_ver['ver']}"
            match _ver["type"]:
                case "REL":
                    _path = DATA_PATH
                case "CBT3":
                    _path = CBT3_DATA_PATH
            if _ver.get("hash"):
                _repo = Repo(_path)
                assert not _repo.is_dirty()
                for dir in [
                    Path("TextMap"),
                    Path("Readable"),
                    Path("Subtitle"),
                ]:
                    _repo.git.restore("--source", _ver["hash"], dir)
            # TODO: data ingest
            for _lang in LANGS:
                if curr_textmap := extract_textmap(
                    _ver["ver"], _lang, _path / "TextMap"
                ):
                    text_data[_lang].append(pl.concat(curr_textmap))
                if not (
                    curr_readable := extract_readable(
                        _ver["ver"], _lang, _path / "Readable" / _lang
                    )
                ).is_empty():
                    text_data[_lang].append(curr_readable)
            if _ver.get("hash"):
                _repo.git.clean("-fd")
                _repo.git.reset("HEAD", "--hard")
            bar.update()
    text_data = {lang: pl.concat(data) for lang, data in text_data.items()}
    {lang: len(data) for lang, data in text_data.items()}
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
