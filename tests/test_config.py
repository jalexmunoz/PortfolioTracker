from importlib import reload
from pathlib import Path

import portfolio_tracker_v2.config as config_module


def test_config_loads_repo_root_dotenv(monkeypatch):
    calls = []

    def fake_load_dotenv(path):
        calls.append(Path(path))
        return True

    monkeypatch.setattr("dotenv.load_dotenv", fake_load_dotenv)
    reload(config_module)

    assert calls == [config_module.PACKAGE_ROOT.parent / ".env"]
