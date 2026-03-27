import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")

with app.setup:
    import os
    from pathlib import Path

    import marimo as mo
    import orjson
    import polars as pl
    from pydantic import (
        BaseModel,
        ConfigDict,
        Field,
        ValidationError,
    )

    from utils import get_textmap

    DATA_PATH = Path(os.environ["DATA_PATH"])
    LANGS = os.environ["LANGS"].split(",")
    VERSION = os.environ["VERSION"]


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Dialogue Parser
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
    DATA_PATH, LANGS, VERSION
    return


@app.function
def load_file(
    file_path: Path, translation: tuple[tuple[str, str]] | None = None
) -> str:
    with open(file_path, "r") as file:
        file_content = file.read()
        if translation:
            for new, old in translation:
                file_content = file_content.replace(old, new)
    return file_content


@app.cell
def _():
    translation = tuple(
        pl.read_csv(Path(f"translation/{VERSION}.csv")).iter_rows()
    )
    translation
    return (translation,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## NpcExcelConfigData
    """)
    return


@app.cell
def _():
    npc_df = pl.read_json(
        DATA_PATH / "ExcelBinOutput/NpcExcelConfigData.json",
        schema={
            "id": pl.String,
            "nameTextMapHash": pl.String,
        },
    ).rename({"id": "talkRoleId", "nameTextMapHash": "talkRoleIdName"})
    npc_df
    return (npc_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## GadgetExcelConfigData
    """)
    return


@app.cell
def _():
    gadget_df = pl.read_json(
        DATA_PATH / "ExcelBinOutput/GadgetExcelConfigData.json",
        schema={
            "id": pl.String,
            "interactNameTextMapHash": pl.String,
        },
    ).rename({"id": "talkRoleId", "interactNameTextMapHash": "talkRoleIdName"})
    gadget_df
    return (gadget_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## MainQuestExcelConfigData
    """)
    return


@app.cell
def _():
    main_quest_df = pl.read_json(
        DATA_PATH / "ExcelBinOutput/MainQuestExcelConfigData.json",
        schema={
            "id": pl.Int64,
            "titleTextMapHash": pl.String,
        },
    ).rename({"id": "questId", "titleTextMapHash": "questIdName"})
    main_quest_df
    return (main_quest_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## AnecdoteExcelConfigData
    """)
    return


@app.cell
def _(translation):
    storyboard_df = (
        pl.DataFrame(
            orjson.loads(
                load_file(
                    DATA_PATH / "ExcelBinOutput/AnecdoteExcelConfigData.json",
                    translation,
                )
            ),
            schema={"id": pl.List(pl.Int64), "name": pl.String},
        )
        .explode("id")
        .rename({"id": "questId", "name": "questIdName"})
    )
    storyboard_df
    return (storyboard_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## NewActivityExcelConfigData
    """)
    return


@app.cell
def _():
    activity_name_df = pl.read_json(
        DATA_PATH / "ExcelBinOutput/NewActivityExcelConfigData.json",
        schema={
            "activityId": pl.Int64,
            "nameTextMapHash": pl.String,
        },
    ).rename({"nameTextMapHash": "activityIdName"})
    activity_name_df
    return (activity_name_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## ChapterExcelConfigData
    """)
    return


@app.cell
def _():
    chapter_name_df = pl.read_json(
        DATA_PATH / "ExcelBinOutput/ChapterExcelConfigData.json",
        schema={
            "id": pl.Int64,
            "chapterTitleTextMapHash": pl.String,
            "chapterNumTextMapHash": pl.String,
        },
    ).rename(
        {
            "id": "chapterId",
            "chapterTitleTextMapHash": "chapterTitle",
            "chapterNumTextMapHash": "chapterNum",
        }
    )
    chapter_name_df
    return (chapter_name_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## ReminderExcelConfigData
    """)
    return


@app.cell
def _():
    reminder_df = pl.read_json(
        DATA_PATH / "ExcelBinOutput/ReminderExcelConfigData.json",
        schema={
            "id": pl.Int64,
            "speakerTextMapHash": pl.String,
            "contentTextMapHash": pl.String,
            "style": pl.String,
            "nextReminderId": pl.Int64,
        },
    ).rename(
        {
            "speakerTextMapHash": "talkRoleIdName",
            "contentTextMapHash": "talkContent",
            "style": "talkRoleType",
        }
    )
    reminder_df
    return (reminder_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## BinOutput/Talk & BinOutput/Quest
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Data Modeling
    """)
    return


@app.class_definition
class TalkRole(BaseModel):
    id: str = Field(serialization_alias="talkRoleId")
    type: str = Field(serialization_alias="talkRoleType")


@app.class_definition
class Dialog(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    id: int
    talkContentTextMapHash: str | None = Field(
        default=None, serialization_alias="talkContent"
    )
    talkRoleNameTextMapHash: str | None = Field(
        default=None, serialization_alias="talkRoleName"
    )
    talkTitleTextMapHash: str | None = Field(
        default=None, serialization_alias="talkTitle"
    )
    talkRole: TalkRole


@app.class_definition
class TalkFile(BaseModel):
    type: str | None = Field(default=None)
    talkId: int
    dialogList: list[Dialog] = Field(min_length=1)


@app.class_definition
class Talk(BaseModel):
    questId: int | None = Field(default=None)
    id: int = Field(serialization_alias="talkId")


@app.class_definition
class ActivityTalk(Talk):
    questId: int | None = Field(default=None, serialization_alias="activityId")


@app.class_definition
class QuestFile(BaseModel):
    id: int = Field(serialization_alias="questIdOuter")
    type: str
    chapterId: int | None = Field(default=None)
    activityId: int | None = Field(default=None)
    dialogList: list[Dialog] = Field(default=[])
    talks: list[Talk] = Field(default=[])


@app.class_definition
class ActivityGroupFile(BaseModel):
    talks: list[ActivityTalk]


@app.class_definition
class FreeGroupFile(BaseModel):
    talkId: int
    type: str
    dialogList: list[Dialog]


@app.class_definition
class GadgetGroupFile(BaseModel):
    talks: list[Talk]


@app.class_definition
class NpcGroupFile(BaseModel):
    talks: list[Talk]


@app.class_definition
class StoryboardGroupFile(BaseModel):
    talks: list[Talk]


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Extraction
    """)
    return


@app.cell
def _(translation):
    def parse_files(files: list[Path], model: BaseModel) -> tuple[list, list]:
        valid_data = []
        error_data = []
        with mo.status.progress_bar(total=len(files), remove_on_exit=True) as bar:
            for file in files:
                relative_path = str(file.relative_to(DATA_PATH / "BinOutput"))
                try:
                    file_content = load_file(file, translation)
                    file_data = model.model_validate_json(file_content).model_dump(
                        by_alias=True
                    )
                    file_data.update({"path": relative_path})
                    valid_data.append(file_data)
                except ValidationError:
                    error_data.append(
                        {"path": relative_path, "content": file_content}
                    )
                bar.update()
        return (valid_data, sorted(error_data, key=lambda data: data["path"]))

    return (parse_files,)


@app.function
def expand_talks(df: pl.DataFrame) -> pl.DataFrame:
    return df.explode("talks").unnest("talks")


@app.cell
def _(parse_files):
    _talk_data, talk_errors = parse_files(
        list(
            set((DATA_PATH / "BinOutput/Talk").rglob("*.json"))
            - set((DATA_PATH / "BinOutput/Talk").rglob("*Group/*.json"))
        ),
        TalkFile,
    )
    talk_df = pl.DataFrame(_talk_data)
    len(talk_errors), talk_df
    return (talk_df,)


@app.cell
def _(parse_files):
    _quest_data, quest_errors = parse_files(
        list((DATA_PATH / "BinOutput/Quest").rglob("*.json")),
        QuestFile,
    )
    _quest_data_df = pl.DataFrame(_quest_data).filter(
        (pl.col.dialogList != []) | (pl.col.talks != [])
    )
    quest_dialog_df = (
        _quest_data_df.drop("talks")
        .rename({"questIdOuter": "questId"})
        .filter(pl.col.dialogList != [])
    )
    quest_talk_df = _quest_data_df.drop("dialogList").pipe(expand_talks)
    len(quest_errors), quest_dialog_df, quest_talk_df
    return quest_dialog_df, quest_talk_df


@app.cell
def _(parse_files):
    _activity_grp_data, activity_grp_errors = parse_files(
        list((DATA_PATH / "BinOutput/Talk/ActivityGroup").rglob("*.json")),
        ActivityGroupFile,
    )
    activity_grp_df = pl.DataFrame(_activity_grp_data).pipe(expand_talks)
    len(activity_grp_errors), activity_grp_df
    return (activity_grp_df,)


@app.cell
def _(parse_files):
    _free_grp_data, free_grp_errors = parse_files(
        list((DATA_PATH / "BinOutput/Talk/FreeGroup").rglob("*.json")),
        FreeGroupFile,
    )
    free_grp_df = pl.DataFrame(_free_grp_data)
    len(free_grp_errors), free_grp_df
    return (free_grp_df,)


@app.cell
def _(parse_files):
    _gadget_grp_data, gadget_grp_errors = parse_files(
        list((DATA_PATH / "BinOutput/Talk/GadgetGroup").rglob("*.json")),
        GadgetGroupFile,
    )
    gadget_grp_df = pl.DataFrame(_gadget_grp_data).pipe(expand_talks)
    len(gadget_grp_errors), gadget_grp_df
    return (gadget_grp_df,)


@app.cell
def _(parse_files):
    _npc_grp_data, npc_grp_errors = parse_files(
        list((DATA_PATH / "BinOutput/Talk/NpcGroup").rglob("*.json")),
        NpcGroupFile,
    )
    npc_grp_df = pl.DataFrame(_npc_grp_data).pipe(expand_talks)
    len(npc_grp_errors), npc_grp_df
    return (npc_grp_df,)


@app.cell
def _(parse_files):
    _storyboard_grp_data, storyboard_grp_errors = parse_files(
        list((DATA_PATH / "BinOutput/Talk/StoryboardGroup").rglob("*.json")),
        StoryboardGroupFile,
    )
    storyboard_grp_df = pl.DataFrame(_storyboard_grp_data).pipe(expand_talks)
    len(storyboard_grp_errors), storyboard_grp_df
    return (storyboard_grp_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Transformation
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Dialogues
    """)
    return


@app.cell
def _(free_grp_df, quest_dialog_df, talk_df):
    _subset = [
        "dialogList",
        "talkId",
        "questId",
        "activityId",
        "chapterId",
        "type",
        "path",
    ]
    dialog_full_df = (
        pl.DataFrame(
            pl.concat(
                [
                    talk_df.with_columns(
                        questId=pl.lit(None, dtype=pl.Int64),
                        activityId=pl.lit(None, dtype=pl.Int64),
                        chapterId=pl.lit(None, dtype=pl.Int64),
                    ).select(_subset),
                    quest_dialog_df.with_columns(
                        talkId=pl.lit(None, dtype=pl.Int64)
                    ).select(_subset),
                    free_grp_df.with_columns(
                        questId=pl.lit(None, dtype=pl.Int64),
                        activityId=pl.lit(None, dtype=pl.Int64),
                        chapterId=pl.lit(None, dtype=pl.Int64),
                    ).select(_subset),
                ]
            ),
        )
        .explode("dialogList")
        .unnest("dialogList")
        .unnest("talkRole")
        .select(
            "id",
            "talkRoleType",
            "talkRoleId",
            "talkRoleName",
            "talkTitle",
            "talkContent",
            "talkId",
            "questId",
            "activityId",
            "chapterId",
            "type",
            "path",
        )
    )
    dialog_full_df
    return (dialog_full_df,)


@app.cell
def _(dialog_full_df):
    dialog_df = (
        (
            dialog_full_df.drop("path").with_columns(
                talkRoleId=pl.col.talkRoleId.str.strip_chars().replace(
                    {"0": None}
                ),
                talkRoleName=pl.col.talkRoleName.replace({"0": None}),
                talkTitle=pl.col.talkTitle.replace({"0": None}),
                talkContent=pl.col.talkContent.replace({"0": None}),
                talkId=pl.col.talkId.replace({0: None}),
                activityId=pl.col.activityId.replace({0: None}),
                chapterId=pl.col.chapterId.replace({0: None}),
                type=pl.col.type.replace({"QUEST": "1QUEST"}),
            )
        )
        .unique()
        .sort(
            "talkId",
            "id",
            "talkRoleId",
            "talkRoleName",
            "talkTitle",
            "talkContent",
            "questId",
            "activityId",
            "chapterId",
            "type",
        )
        .group_by(
            [
                "talkId",
                "id",
                "talkRoleId",
                "questId",
                "activityId",
                "chapterId",
            ],
            maintain_order=True,
        )
        .agg(pl.all().last())
    )
    dialog_df
    return (dialog_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Metadata Matching
    """)
    return


@app.cell
def _(gadget_grp_df, npc_grp_df, quest_talk_df, storyboard_grp_df):
    _subset = ["talkId", "questId"]
    quest_id_df = (
        pl.concat(
            [
                quest_talk_df.select(_subset),
                gadget_grp_df.select(_subset),
                npc_grp_df.select(_subset),
                storyboard_grp_df.select(_subset),
            ]
        )
        .with_columns(questId=pl.col.questId.replace({0: None}))
        .drop_nulls()
        .unique()
    )
    quest_id_df
    return (quest_id_df,)


@app.cell
def _(activity_grp_df, quest_talk_df):
    _subset = ["talkId", "activityId"]
    activity_id_df = (
        pl.concat(
            [
                quest_talk_df.select(_subset),
                activity_grp_df.select(_subset),
            ]
        )
        .with_columns(activityId=pl.col.activityId.replace({0: None}))
        .drop_nulls()
        .unique()
    )
    _drop = (
        activity_id_df.filter(pl.col.talkId.is_duplicated())
        .get_column("talkId")
        .value_counts()
        .filter(pl.col.count > 2)
        .get_column("talkId")
        .to_list()
    )
    activity_id_df = (
        activity_id_df.filter(~(pl.col.talkId.is_in(_drop)))
        .sort("talkId", "activityId")
        .group_by("talkId", maintain_order=True)
        .agg(pl.all().last())
    )
    activity_id_df
    return (activity_id_df,)


@app.cell
def _(quest_talk_df):
    chapter_id_df = (
        quest_talk_df.select("talkId", "chapterId")
        .with_columns(chapterId=pl.col.chapterId.replace({0: None}))
        .drop_nulls()
        .unique()
    )
    chapter_id_df
    return (chapter_id_df,)


@app.cell
def _(activity_grp_df, quest_talk_df):
    _subset = ["talkId", "type"]
    type_df = (
        pl.concat(
            [
                quest_talk_df.select(_subset),
                activity_grp_df.with_columns(type=pl.lit("ACTIVITY")).select(
                    _subset
                ),
            ]
        )
        .drop_nulls()
        .unique()
        .filter(~(pl.col.talkId.is_duplicated()) | (pl.col.type != "FREE"))
    )
    type_df
    return (type_df,)


@app.cell
def _(activity_id_df, chapter_id_df, dialog_df, quest_id_df, type_df):
    dialog_matched_df = (
        dialog_df.with_columns(
            questId=pl.col.talkId.replace_strict(
                quest_id_df.get_column("talkId"),
                quest_id_df.get_column("questId"),
                default=pl.col.questId,
            ),
            activityId=pl.col.talkId.replace_strict(
                activity_id_df.get_column("talkId"),
                activity_id_df.get_column("activityId"),
                default=pl.col.activityId,
            ),
            chapterId=pl.col.talkId.replace_strict(
                chapter_id_df.get_column("talkId"),
                chapter_id_df.get_column("chapterId"),
                default=pl.col.chapterId,
            ),
            type=pl.col.talkId.replace_strict(
                type_df.get_column("talkId"),
                type_df.get_column("type"),
                default=pl.col.type,
            ),
        )
        .filter(pl.col.type != "1QUEST")
        .sort(
            "talkId",
            "id",
            "questId",
            "activityId",
            "chapterId",
        )
    )
    dialog_matched_df
    return (dialog_matched_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Name Matching
    """)
    return


@app.cell
def _(gadget_df, npc_df):
    talk_role_df = pl.concat([npc_df, gadget_df])
    talk_role_df
    return (talk_role_df,)


@app.cell
def _(main_quest_df, storyboard_df):
    quest_name_df = pl.concat([main_quest_df, storyboard_df])
    quest_name_df
    return (quest_name_df,)


@app.cell
def _(
    activity_name_df,
    chapter_name_df,
    dialog_matched_df,
    quest_name_df,
    talk_role_df,
):
    dialog_named_df = (
        dialog_matched_df.join(talk_role_df, on="talkRoleId", how="left")
        .join(quest_name_df, on="questId", how="left")
        .join(activity_name_df, on="activityId", how="left")
        .join(chapter_name_df, on="chapterId", how="left")
    ).select(
        "id",
        "talkRoleId",
        "talkRoleIdName",
        "talkRoleName",
        "talkTitle",
        "talkContent",
        "talkRoleType",
        "talkId",
        "questId",
        "questIdName",
        "activityId",
        "activityIdName",
        "chapterId",
        "chapterTitle",
        "chapterNum",
        "type",
    )
    dialog_named_df
    return (dialog_named_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Reminders
    """)
    return


@app.cell
def _(reminder_df):
    _reminder_data = reminder_df.to_dicts()
    _groups = []
    for _reminder in _reminder_data:
        for _group in _groups:
            if _reminder["id"] in _group["ids"]:
                _group["data"].append(_reminder)
                if _reminder["nextReminderId"] != 0:
                    _group["ids"].add(_reminder["nextReminderId"])
                break
        else:
            _groups.append({"ids": {_reminder["id"]}, "data": [_reminder]})
            if _reminder["nextReminderId"] != 0:
                _groups[-1]["ids"].add(_reminder["nextReminderId"])
    reminder_grp_df = (
        pl.DataFrame(_groups)
        .with_row_index(name="talkId", offset=1)
        .with_columns(pl.col.talkId.reverse().cast(pl.Int32).neg())
        .explode("data")
        .unnest("data")
        .select("id", "talkRoleIdName", "talkContent", "talkRoleType", "talkId")
    )
    reminder_grp_df
    return (reminder_grp_df,)


@app.cell
def _(dialog_named_df, reminder_grp_df):
    dialog_final_df = pl.concat(
        [
            reminder_grp_df.select(
                pl.col.id,
                pl.lit(None, dtype=pl.String).alias("talkRoleId"),
                pl.col.talkRoleIdName,
                pl.lit(None, dtype=pl.String).alias("talkRoleName"),
                pl.lit(None, dtype=pl.String).alias("talkTitle"),
                pl.col.talkContent,
                pl.col.talkRoleType,
                pl.col.talkId.cast(pl.Int64),
                pl.lit(None, dtype=pl.Int64).alias("questId"),
                pl.lit(None, dtype=pl.String).alias("questIdName"),
                pl.lit(None, dtype=pl.Int64).alias("activityId"),
                pl.lit(None, dtype=pl.String).alias("activityIdName"),
                pl.lit(None, dtype=pl.Int64).alias("chapterId"),
                pl.lit(None, dtype=pl.String).alias("chapterTitle"),
                pl.lit(None, dtype=pl.String).alias("chapterNum"),
                pl.lit("REMINDER").alias("type"),
            ),
            dialog_named_df,
        ]
    )
    dialog_final_df
    return (dialog_final_df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Output
    """)
    return


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
def _(dialog_final_df):
    _output_path = Path("output")
    os.makedirs(_output_path, exist_ok=True)
    for _lang in LANGS:
        _output_df = dialog_final_df.pipe(
            resolve_text, get_textmap(DATA_PATH / "TextMap", _lang)
        )
        _output_path_base = _output_path / f"GI_Talk_{_lang}_{VERSION}"
        _output_df.write_parquet(
            _output_path_base.with_suffix(".parquet"),
        )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
