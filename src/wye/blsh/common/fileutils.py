import json
import shutil
from pathlib import Path

def create_file(path: Path, contents) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(contents)
        tmp.replace(path)
        return True
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return False


def create_json(path: Path, data, **kwargs) -> bool:
    if data:
        json_dumps = json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            **kwargs
        )
        return create_file(path, json_dumps)
    return False