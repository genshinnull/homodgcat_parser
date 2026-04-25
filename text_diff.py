import os
from pathlib import Path

import polars as pl

VERSION = os.environ["VERSION"]
VERSION_OLD = os.environ["VERSION_OLD"]
LANGS = os.environ["LANGS"].split(",")
INPUT_PATH = Path("staging/text1")
OUTPUT_PATH = Path("product")
BETA = os.environ.get("BETA", False)

os.makedirs(OUTPUT_PATH, exist_ok=True)

for lang in LANGS:
    df = pl.read_parquet(INPUT_PATH / f"GI_Text_{lang}_{VERSION}.parquet")
    versions = df.get_column("version").unique().sort().to_list()
    old_df = df.filter(pl.col.version == versions[-2])
    new_df = df.filter(pl.col.version == versions[-1])

    len_df = (
        old_df.group_by("type")
        .len(name=VERSION_OLD)
        .join(new_df.group_by("type").len(name=VERSION), on="type")
        .sort("type")
    )

    readable_df = (
        new_df.filter(
            pl.col.type == "Readable",
            ~pl.col.key.is_in(
                old_df.filter(pl.col.type == "Readable").get_column("key").to_list()
            ),
        )
        .select("key", "paged", "book", "letter")
        .sort("key")
    )

    subtitle_df = (
        new_df.filter(
            pl.col.type == "Subtitle",
            ~pl.col.key.is_in(
                old_df.filter(pl.col.type == "Subtitle").get_column("key").to_list()
            ),
        )
        .select("key")
        .sort("key")
    )

    with (
        open(
            OUTPUT_PATH
            / (
                f"GI_Text_Beta_{lang}_Diff_Stats.md"
                if BETA
                else f"GI_Text_{lang}_Diff_Stats.md"
            ),
            "w",
        ) as f,
        pl.Config(
            tbl_rows=-1,
            tbl_width_chars=-1,
            fmt_str_lengths=80,
            tbl_formatting="MARKDOWN",
            tbl_hide_column_data_types=True,
            tbl_hide_dataframe_shape=True,
        ),
    ):
        f.write(
            f"# {VERSION_OLD} - {VERSION} Text Diff Report"
            + "\n\n## Total Entry Counts\n\n"
            + str(len_df)
            + "\n\n## New Readable Entries\n\n"
            + str(readable_df)
            + "\n\n## New Subtitle Entries\n\n"
            + str(subtitle_df)
            + "\n"
        )

    new_df.drop("version").sort("value", "type", "key").write_parquet(
        OUTPUT_PATH
        / (f"GI_Text_Beta_{lang}.parquet" if BETA else f"GI_Text_{lang}.parquet")
    )
