import marimo

__generated_with = "0.20.4"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Fieldname Translation
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Setup
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import csv
    import os
    from pathlib import Path

    import orjson

    return Path, csv, orjson, os


@app.cell
def _(Path, os):
    DATA_PATH = Path(os.environ["DATA_PATH"])
    VERSION = Path(os.environ["VERSION"])
    DATA_PATH, VERSION
    return DATA_PATH, VERSION


@app.cell
def _(DATA_PATH, Path, orjson):
    def load_json(path: Path):
        with open(DATA_PATH / path) as f:
            return orjson.loads(f.read())

    return (load_json,)


@app.cell
def _():
    output = [("type", "damageRatio"), ("id", "_id"), ("type", "_type")]
    return (output,)


@app.function
def find(data: dict, hint):
    for field in data.items():
        if field[1] == hint:
            return field[0]
    else:
        raise ValueError


@app.function
def find_sub(data: dict, hint):
    for field in data.items():
        if (
            isinstance(field[1], list)
            and len(field[1]) > 0
            and isinstance(field[1][0], dict)
        ):
            for sub_field in field[1][0].items():
                if sub_field[1] == hint:
                    return field
    else:
        raise ValueError


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## ExcelBinOutput/AnecdoteExcelConfigData.json
    """)
    return


@app.cell
def _(load_json):
    storyboard_sample = load_json("ExcelBinOutput/AnecdoteExcelConfigData.json")
    type(storyboard_sample), len(storyboard_sample)
    return (storyboard_sample,)


@app.cell
def _(output, storyboard_sample):
    output.append(("id", find(storyboard_sample[0], [510000101])))
    output.append(("name", find(storyboard_sample[0], 1253955835)))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## BinOutput/Quest/5024.json
    """)
    return


@app.cell
def _(load_json):
    quest_sample = load_json("BinOutput/Quest/5024.json")
    type(quest_sample), len(quest_sample)
    return (quest_sample,)


@app.cell
def _(output, quest_sample):
    output.append(("type", find(quest_sample, "AQ")))
    output.append(("id", find(quest_sample, 5024)))
    output.append(("chapterId", find(quest_sample, 1504)))
    return


@app.cell
def _(output, quest_sample):
    dialogList = find_sub(quest_sample, "TALK_SHOW_DEFAULT")
    output.append(("dialogList", dialogList[0]))
    output.append(("talkContentTextMapHash", find(dialogList[1][0], 837085410)))
    output.append(("talkRoleNameTextMapHash", find(dialogList[1][1], 3375747035)))
    output.append(("talkTitleTextMapHash", find(dialogList[1][1], 4250729636)))
    output.append(
        (
            "talkRole",
            find(
                dialogList[1][0],
                {"_id": "", "_roleId": 0, "_type": "TALK_ROLE_NONE"},
            ),
        )
    )
    return


@app.cell
def _(output, quest_sample):
    talks = find_sub(quest_sample, "PLAY_MODE_SINGLE")
    output.append(("talks", talks[0]))
    output.append(("questId", find(talks[1][0], 5024)))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## BinOutput/Talk/Quest/30610.json
    """)
    return


@app.cell
def _(load_json):
    talk_sample = load_json("BinOutput/Talk/Quest/30610.json")
    type(talk_sample), len(talk_sample)
    return (talk_sample,)


@app.cell
def _(output, talk_sample):
    output.append(("talkId", find(talk_sample, 30610)))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## BinOutput/Quest/40020.json
    """)
    return


@app.cell
def _(load_json, talk_sample):
    activity_sample = load_json("BinOutput/Quest/40020.json")
    type(activity_sample), len(talk_sample)
    return (activity_sample,)


@app.cell
def _(activity_sample, output):
    output.append(("activityId", find(activity_sample, 2008)))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Output
    """)
    return


@app.cell
def _(Path, VERSION, csv, os, output):
    _output_path = Path("translation")
    os.makedirs(_output_path, exist_ok=True)
    with open((_output_path / VERSION).with_suffix(".csv"), "w") as f:
        writer = csv.writer(f)
        writer.writerow(["new", "old"])
        writer.writerows(output)
    return


if __name__ == "__main__":
    app.run()
