import os

import polars as pl

LANGS  = os.environ["LANGS"].split(",")

for lang in LANGS:
    (
        pl.read_parquet(f"product/GI_Talk_{lang}.parquet").select(
            "talkId",
            "id",
            "talkRoleIdName",
            "talkRoleName",
            "talkTitle",
            "talkContent",
            "talkRoleType",
            "questId",
            "questIdName",
            "activityIdName",
            "chapterId",
            "chapterTitle",
            "chapterNum",
            "type",
            "talkIdExpandable",
            "questIdExpandable",
            "new",
        )
        .write_parquet(f"product/V2_Talk_{lang}.parquet")
    )
