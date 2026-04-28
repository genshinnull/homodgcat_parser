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
    INPUT_PATH = Path("staging/talk0")
    OUTPUT_PATH = Path("staging/talk1")


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
    return pros, textmap


@app.function
def map_text(expr: pl.Expr, textmap: dict[str, str]) -> pl.Expr:
    return expr.replace_strict(textmap, default=None)


@app.function
def resolve_text(
    df: pl.DataFrame,
    textmap: dict[str, str],
):
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
def _(locs, pros):
    def enhance_text(df: pl.DataFrame, lang: str):
        return df.with_columns(
            talkRoleIdName=pl.when(pl.col.talkRoleType == "TALK_ROLE_PLAYER")
            .then(pl.lit(locs["SPEAKER_TALK_ROLE_PLAYER"][lang]))
            .when(pl.col.talkRoleType == "TALK_ROLE_MATE_AVATAR")
            .then(pl.lit(locs["SPEAKER_TALK_ROLE_MATE_AVATAR"][lang]))
            .otherwise(
                pl.col.talkRoleIdName.pipe(replace_terms, locs, pros, lang)
            ),
            talkRoleName=pl.col.talkRoleName.pipe(replace_terms, locs, pros, lang),
            talkContent=pl.col.talkContent.pipe(process_whitespace)
            .pipe(remove_tags)
            .pipe(replace_terms, locs, pros, lang),
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
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    for _lang in LANGS:
        pl.scan_parquet(INPUT_PATH / f"GI_Talk_{VERSION}.parquet").pipe(
            resolve_text, textmap[_lang]
        ).pipe(enhance_text, _lang).sink_parquet(
            OUTPUT_PATH / f"GI_Talk_{_lang}_{VERSION}.parquet"
        )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
