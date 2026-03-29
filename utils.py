import re
from pathlib import Path

import orjson
import polars as pl


def get_textmap(
    dir: Path,
    lang: str,
) -> dict[str, str]:
    textmap = dict()
    files = list(dir.glob("*.json"))
    for file in files:
        if re.match(rf"(Text{lang}|TextMap(_Medium)?{lang}(_\d)?)", file.stem):
            with open(file) as f:
                textmap.update(orjson.loads(f.read()))
    return textmap


def replace_term(expr: pl.Expr, localization: dict, lang: str) -> pl.Expr:
    return (
        pl.when(expr.str.contains(r"(?s)^#.*\{"))
        .then(
            expr.str.slice(1)
            .str.replace_all(
                r"\{REALNAME\[ID\(1\)(\|\w+\(\w+\))?\]\}",
                localization["SPEAKER_REALNAME_ID_1"][lang],
            )
            .str.replace_all(
                r"\{REALNAME\[ID\(2\)(\|\w+\(\w+\))?\]\}",
                localization["SPEAKER_REALNAME_ID_2"][lang],
            )
            .str.replace_all(
                r"\{NICKNAME\}", localization["SPEAKER_TALK_ROLE_PLAYER"][lang]
            )
        )
        .otherwise(expr)
    )


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
