import json
import os
from dataclasses import dataclass
from typing import Optional

from flask import current_app

MODE_TEST = "TEST"
MODE_PROD = "PROD"
_ALLOWED_MODES = {MODE_TEST, MODE_PROD}
_STATE_FILE = "gui_state.json"


@dataclass(frozen=True)
class ActiveDbContext:
    db_path: str
    mode: str



def _state_path() -> str:
    os.makedirs(current_app.instance_path, exist_ok=True)
    return os.path.join(current_app.instance_path, _STATE_FILE)



def load_active_db() -> Optional[ActiveDbContext]:
    state_file = _state_path()
    if not os.path.exists(state_file):
        return None

    with open(state_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    db_path = payload.get("db_path", "").strip()
    mode = payload.get("mode", "").strip().upper()
    if not db_path or mode not in _ALLOWED_MODES:
        return None

    return ActiveDbContext(db_path=db_path, mode=mode)



def set_active_db(db_path: str, mode: str) -> ActiveDbContext:
    clean_path = os.path.abspath(os.path.expanduser((db_path or "").strip()))
    clean_mode = (mode or "").strip().upper()

    if not clean_path:
        raise ValueError("DB path is required")
    if clean_mode not in _ALLOWED_MODES:
        raise ValueError("Mode must be TEST or PROD")

    payload = {"db_path": clean_path, "mode": clean_mode}
    with open(_state_path(), "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return ActiveDbContext(db_path=clean_path, mode=clean_mode)



def clear_active_db() -> None:
    state_file = _state_path()
    if os.path.exists(state_file):
        os.remove(state_file)
