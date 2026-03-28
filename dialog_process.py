import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import polars as pl
    import os
    from utils import get_textmap
    from pathlib import Path

    DATA_PATH = Path(os.environ["DATA_PATH"])
    LANGS = os.environ["LANGS"].split(",")
    VERSION = os.environ["VERSION"]


@app.cell
def _():
    DATA_PATH, LANGS, VERSION
    return


@app.cell
def _():
    textmap = {}
    for _lang in LANGS:
        textmap[_lang] = get_textmap(DATA_PATH / "TextMap", _lang)
    return (textmap,)


@app.function
def resolve_text(
    dialog_df: pl.DataFrame,
    textmap: dict[str, str],
) -> pl.DataFrame:
    return dialog_df.with_columns(
        talkRoleIdName=pl.when(pl.col.talkRoleId.str.contains(r"\D"))
        .then("talkRoleId")
        .otherwise(
            pl.col.talkRoleIdName.replace_strict(textmap, default=None)
        ),
        talkRoleName=pl.col.talkRoleName.replace_strict(textmap, default=None),
        talkTitle=pl.col.talkTitle.replace_strict(textmap, default=None),
        talkContent=pl.col.talkContent.replace_strict(textmap, default=None)
        .str.replace_all(r"\\n", "\n")
        .str.strip_chars(),
        questIdName=pl.col.questIdName.replace_strict(textmap, default=None),
        activityIdName=pl.col.activityIdName.replace_strict(
            textmap, default=None
        ),
        chapterTitle=pl.col.chapterTitle.replace_strict(textmap, default=None),
        chapterNum=pl.col.chapterNum.replace_strict(textmap, default=None),
    )


@app.cell
def _(textmap):
    _input_path = Path("staging/talk0")
    _output_path = Path("staging/talk1")
    os.makedirs(_output_path, exist_ok=True)
    for _lang in LANGS:
        pl.read_parquet(_input_path / f"GI_Talk_{VERSION}.parquet").pipe(
            resolve_text, textmap[_lang]
        ).write_parquet(_output_path / f"GI_Talk_{_lang}_{VERSION}.parquet")
    return


@app.cell
def _():
    # pronouns = (
    #     pl.read_json(
    #         DATA_PATH / "ExcelBinOutput/ManualTextMapConfigData.json",
    #         schema={
    #             "textMapId": pl.String,
    #             "textMapContentTextMapHash": pl.String,
    #         },
    #     )
    #     .filter(pl.col.textMapId.str.contains("PRONOUN", literal=True))
    #     .with_columns(
    #         pl.col.textMapContentTextMapHash.replace_strict(
    #             textmap, default=None
    #         ).drop_nulls()
    #     ).to_dicts()
    # )
    # pronouns = {pronoun["textMapId"]: pronoun["textMapContentTextMapHash"] for pronoun in pronouns}
    # pronouns
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
