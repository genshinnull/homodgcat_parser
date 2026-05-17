import os
from pathlib import Path

import polars as pl

VERSION = os.environ["TEXT_VERSION"]
OLD_VERSION = os.environ["TEXT_OLD_VERSION"]
LANGS = os.environ["LANGS"].split(",")
INPUT_PATH = Path("staging/text1")
OUTPUT_PATH = Path("product")

OUTPUT_PATH.mkdir(exist_ok=True)

for lang in LANGS:
    df = pl.read_parquet(
        INPUT_PATH / f"GI_Text_{lang}_{VERSION.replace('.', '_')}.parquet"
    )
    old_df = df.filter(pl.col.version == OLD_VERSION)
    new_df = df.filter(pl.col.version == VERSION)

    len_df = (
        old_df.group_by("type")
        .len(name=OLD_VERSION)
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
        open(OUTPUT_PATH / f"GI_Text_{lang}_Diff_Stats.md", "w") as f,
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
            f"# {OLD_VERSION} - {VERSION} Text Diff Report"
            + "\n\n## Total Entry Counts\n\n"
            + str(len_df)
            + "\n\n## New Readable Entries\n\n"
            + str(readable_df)
            + "\n\n## New Subtitle Entries\n\n"
            + str(subtitle_df)
            + "\n"
        )

    df.group_by(pl.exclude("version")).agg("version").sort(
        "value", "type", "key", "kv_from"
    ).write_parquet(OUTPUT_PATH / f"GI_Text_{lang}.parquet")
