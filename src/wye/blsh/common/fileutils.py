import json
import logging
from typing import Union
from pathlib import Path
from dataclasses import asdict, dataclass, is_dataclass

log = logging.getLogger(__name__)

def create_file(path:Path, contents)->bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(contents)
        tmp.replace(path)
        return True
    except Exception as e:
        log.error(f"파일 저장 실패: {e}")
        tmp.unlink(missing_ok=True)
        return False

def create_json(path:Path, data:Union[dataclass, dict, list])->bool:
    if data:
        return create_file(path, json.dumps(
                    asdict(data) if is_dataclass(data) else
                    {t: asdict(p) for t, p in data.items()} if isinstance(data, dict) else data,
                    ensure_ascii=False,
                    indent=2,
                ))
    return False