import os
from pathlib import Path

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
VERSION = os.environ["TEXT_VERSION"]
version = VERSION.replace(".", "_")
INTPUT_PATH = Path("staging/text0")

with open("localization.json") as _f:
    locs = orjson.loads(_f.read())

textmap = {}
pros = {}
for lang in LANGS:
    textmap[lang] = get_textmap(DATA_PATH / "TextMap", lang)
    pros[lang] = get_pronouns(
        DATA_PATH / "ExcelBinOutput/ManualTextMapConfigData.json",
        lang,
        textmap[lang],
    )

for lang in LANGS:
    (
        pl.scan_parquet(str(INTPUT_PATH / f"GI_Text_{lang}_Single_*.parquet"))
        .filter(pl.col.version <= VERSION)
        .with_columns(
            pl.col.value.pipe(remove_tags)
            .pipe(process_whitespace)
            .pipe(replace_terms, locs, pros, lang),
        )
        .with_columns(
            pl.concat_str(
                "paged",
                "book",
                "letter",
                separator=" - ",
                ignore_nulls=True,
            )
            .replace({"": None})
            .alias("readable_name")
        )
        .drop("paged", "book", "letter")
        .with_columns(
            pl.col("version").min().over("key").alias("k_from"),
            pl.col("version").min().over("value").alias("v_from"),
            pl.col("version").min().over("key", "value").alias("kv_from"),
        )
        .group_by(
            "type", "key", "value", "readable_name", "k_from", "v_from", "kv_from"
        )
        .agg(pl.col("version").sort().alias("versions"))
        .sort("value", "type", "key", "versions")
        .sink_parquet(f"product/V2_Text_{lang}.parquet")
    )
