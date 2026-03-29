import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import os
    from pathlib import Path

    import marimo as mo
    import orjson
    import polars as pl

    from utils import get_textmap, replace_term

    DATA_PATH = Path(os.environ["DATA_PATH"])
    LANGS = os.environ["LANGS"].split(",")
    VERSION = os.environ["VERSION"]


@app.cell
def _():
    DATA_PATH, LANGS, VERSION
    return


@app.cell
def _():
    with open("localization.json") as _f:
        loc = orjson.loads(_f.read())
    return (loc,)


@app.cell
def _():
    textmap = {}
    for _lang in LANGS:
        textmap[_lang] = get_textmap(DATA_PATH / "TextMap", _lang)
    return (textmap,)


@app.function
def map_text(expr: pl.Expr, textmap: dict[str, str]) -> pl.Expr:
    return expr.replace_strict(textmap, default=None)


@app.function
def resolve_text(
    df: pl.DataFrame,
    textmap: dict[str, str],
) -> pl.DataFrame:
    return df.with_columns(
        talkRoleIdName=pl.when(pl.col.talkRoleId.str.contains(r"\D"))
        .then("talkRoleId")
        .otherwise(pl.col.talkRoleIdName.pipe(map_text, textmap)),
        talkRoleName=pl.col.talkRoleName.pipe(map_text, textmap),
        talkTitle=pl.col.talkTitle.pipe(map_text, textmap),
        talkContent=pl.col.talkContent.pipe(map_text, textmap),
        questIdName=pl.col.questIdName.pipe(map_text, textmap),
        activityIdName=pl.col.activityIdName.pipe(map_text, textmap),
        chapterTitle=pl.col.chapterTitle.pipe(map_text, textmap),
        chapterNum=pl.col.chapterNum.pipe(map_text, textmap),
    )


@app.cell
def _(loc):
    def enhance_text(df: pl.DataFrame, lang: str) -> pl.DataFrame:
        return df.with_columns(
            talkRoleIdName=pl.when(pl.col.talkRoleType == "TALK_ROLE_PLAYER")
            .then(pl.lit(loc["SPEAKER_TALK_ROLE_PLAYER"][lang]))
            .when(pl.col.talkRoleType == "TALK_ROLE_MATE_AVATAR")
            .then(pl.lit(loc["SPEAKER_TALK_ROLE_MATE_AVATAR"][lang]))
            .otherwise(pl.col.talkRoleIdName.pipe(replace_term, loc, lang)),
            talkRoleName=pl.col.talkRoleName.pipe(replace_term, loc, lang),
            talkContent=pl.col.talkContent.str.replace_all(r"\\n", "\n")
            .str.strip_chars()
            .pipe(replace_term, loc, lang),
            talkIdExpandable=(
                (pl.col.talkId.is_not_null()) & (pl.len().over("talkId") > 1)
            ),
            questIdExpandable=(
                (pl.col.questId.is_not_null()) & (pl.len().over("questId") > 1)
            ),
        ).with_columns(
            talkRoleIdNameLower=pl.col.talkRoleIdName.str.to_lowercase(),
            talkRoleNameLower=pl.col.talkRoleName.str.to_lowercase(),
            talkTitleLower=pl.col.talkTitle.str.to_lowercase(),
            talkContentLower=pl.col.talkContent.str.to_lowercase(),
        )

    return (enhance_text,)


@app.cell
def _(enhance_text, textmap):
    _input_path = Path("staging/talk0")
    _output_path = Path("staging/talk1")
    os.makedirs(_output_path, exist_ok=True)
    for _lang in LANGS:
        pl.read_parquet(_input_path / f"GI_Talk_{VERSION}.parquet").pipe(
            resolve_text, textmap[_lang]
        ).pipe(enhance_text, _lang).write_parquet(
            _output_path / f"GI_Talk_{_lang}_{VERSION}.parquet"
        )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
