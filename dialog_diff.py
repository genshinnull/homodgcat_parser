import os
from pathlib import Path

import polars as pl

VERSION = os.environ["VERSION"]
VERSION_OLD = os.environ["VERSION_OLD"]
LANGS = os.environ["LANGS"].split(",")
INPUT_PATH = Path("staging/talk1")
OUTPUT_PATH = Path("product")

os.makedirs(OUTPUT_PATH, exist_ok=True)


def condense_col(expr: pl.Expr) -> pl.Expr:
    return expr.drop_nulls().unique().implode().list.sort().list.join(",")


for lang in LANGS:
    old_df = pl.read_parquet(INPUT_PATH / f"GI_Talk_{lang}_{VERSION_OLD}.parquet").drop(
        r"^.*(Expandable|Lower)$"
    )
    new_df = (
        pl.read_parquet(INPUT_PATH / f"GI_Talk_{lang}_{VERSION}.parquet")
        .drop(r"^.*(Expandable|Lower)$")
        .with_columns(new=~pl.col.id.is_in(old_df.get_column("id").unique().to_list()))
    )
    old_in_new_df = new_df.filter(~pl.col.new).drop("new")
    new_in_new_df = new_df.filter(pl.col.new).drop("new")

    len_df = pl.concat(
        [
            old_df.select(pl.len()),
            old_in_new_df.select(pl.len()),
            new_in_new_df.select(pl.len()),
        ]
    ).transpose(column_names=[VERSION_OLD, f"{VERSION}_old", f"{VERSION}_new"])

    null_df = pl.concat(
        [
            old_df.null_count(),
            old_in_new_df.null_count(),
            new_in_new_df.null_count(),
        ]
    ).transpose(
        include_header=True,
        header_name="field",
        column_names=[VERSION_OLD, f"{VERSION}_old", f"{VERSION}_new"],
    )

    unique_df = pl.concat(
        [
            old_df.select(pl.all().n_unique()),
            old_in_new_df.select(pl.all().n_unique()),
            new_in_new_df.select(pl.all().n_unique()),
        ]
    ).transpose(
        include_header=True,
        header_name="field",
        column_names=[VERSION_OLD, f"{VERSION}_old", f"{VERSION}_new"],
    )

    role_df = (
        new_in_new_df.drop_nulls("talkRoleIdName")
        .select("talkRoleIdName", "talkRoleName", "talkTitle", "type")
        .group_by("talkRoleIdName")
        .agg(
            pl.col.talkRoleIdName.len().alias("count"),
            pl.col.talkRoleName.pipe(condense_col),
            pl.col.talkTitle.pipe(condense_col),
            pl.col.type.drop_nulls().pipe(condense_col),
        )
        .sort(["count", "talkRoleIdName"], descending=[True, False])
    )

    with (
        open(OUTPUT_PATH / f"GI_Talk_{lang}_Diff_Stats.md", "w") as f,
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
            f"# {VERSION_OLD} - {VERSION} Dialogue Diff Report"
            + "\n\n## Total Line Counts\n\n"
            + str(len_df)
            + "\n\n## Total Null Counts\n\n"
            + str(null_df)
            + "\n\n## Total Unique Values\n\n"
            + str(unique_df)
            + "\n\n## New Line Counts by Speaker\n\n"
            + str(role_df)
            + "\n"
        )

    new_df.write_parquet(OUTPUT_PATH / f"GI_Talk_{lang}.parquet")
