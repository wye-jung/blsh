from typing import Union
from dataclasses import dataclass, is_dataclass, asdict

def create_file(filepath:Path, contents)->bool:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    tmp = filepath.with_suffix(".tmp")
    try:
        tmp.write_text(contents)
        tmp.replace(filepath)
        return True
    except Exception as e:
        log.error(f"파일 저장 실패: {e}")
        tmp.unlink(missing_ok=True)
        return False

def create_json(filepath:Path, data:Union[dataclass, dict, list)->bool:
    if data:
        return create_file(filepath, json.dumps(
                    asdict(data) if is_dataclass(data) else
                    {t: asdict(p) for t, p in to_save.items()} if isinstance(data, list) else data
                    ensure_ascii=False,
                    indent=2,
                ))
    return False