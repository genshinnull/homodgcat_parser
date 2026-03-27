import re
from pathlib import Path

import orjson


def get_textmap(dir: Path, lang: str, ) -> dict[str, str]:
    textmap = dict()
    files = list(dir.glob("*.json"))
    for file in files:
        if re.match(rf"(Text{lang}|TextMap(_Medium)?{lang}(_\d)?)", file.stem):
            with open(file) as f:
                textmap.update(orjson.loads(f.read()))
    return textmap
