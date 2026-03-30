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


def get_pronouns(path: Path, lang: str, textmap: dict[str, str]) -> dict[str, str]:
    pronouns = (
        pl.read_json(
            path,
            schema={
                "textMapId": pl.String,
                "textMapContentTextMapHash": pl.String,
            },
        )
        .filter(pl.col.textMapId.str.contains("PRONOUN", literal=True))
        .with_columns(
            pl.col.textMapContentTextMapHash.replace_strict(
                textmap, default=None
            ).drop_nulls()
        )
        .to_dicts()
    )
    return {
        pronoun["textMapId"]: pronoun["textMapContentTextMapHash"]
        for pronoun in pronouns
    }


def replace_terms(
    expr: pl.Expr, localization: dict, pronouns: dict[str, str], lang: str
) -> pl.Expr:
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
            .str.replace_many(pronouns[lang])
        )
        .otherwise(expr)
    )
