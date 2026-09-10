import json
from pathlib import Path
import sys
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "plugins"))
airflow_module = types.ModuleType("airflow")
models_module = types.ModuleType("airflow.models")
models_module.Variable = object
sys.modules.setdefault("airflow", airflow_module)
sys.modules.setdefault("airflow.models", models_module)

from helpers import batch_events


def test_office_key_precedes_default(monkeypatch):
    values = {
        "BATCH_EVENTS_API_KEY": "default-test-key",
        "BATCH_EVENTS_API_KEY_SWT": "office-test-key",
    }
    monkeypatch.setattr(
        batch_events, "get_config", lambda name, default=None: values.get(name, default)
    )
    assert batch_events.get_api_key("swt") == "office-test-key"
    assert batch_events.get_api_key("LRL") == "default-test-key"


def test_single_post_uses_existing_api_key_auth(monkeypatch):
    values = {
        "BATCH_EVENTS_API_ROOT": "https://events.example/api",
        "BATCH_EVENTS_API_KEY": "synthetic-key",
    }
    monkeypatch.setattr(
        batch_events, "get_config", lambda name, default=None: values.get(name, default)
    )
    calls = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"id": "job-1"}'

    def urlopen(request, timeout):
        calls.append(request)
        assert timeout == 30
        return Response()

    monkeypatch.setattr(batch_events, "urlopen", urlopen)
    assert batch_events.trigger_job("script-1", "SWT") == {"id": "job-1"}
    assert len(calls) == 1
    assert calls[0].headers["Authorization"] == "apikey synthetic-key"
    assert calls[0].full_url == "https://events.example/api/jobs"
    assert json.loads(calls[0].data) == {"scriptId": "script-1"}


def test_explicit_office_list_does_not_expand_default_key_scope(monkeypatch):
    monkeypatch.setattr(
        batch_events,
        "api_request",
        lambda *args: [
            {"id": "swt-job", "office": "SWT"},
            {"id": "lrl-job", "office": "LRL"},
        ],
    )
    assert batch_events.get_scheduled_scripts(["SWT"]) == [
        {"id": "swt-job", "office": "SWT"}
    ]


def test_failed_office_does_not_block_other_offices(monkeypatch):
    def scripts(office):
        if office == "SWT":
            raise RuntimeError("Synthetic request failure")
        return [{"id": "lrl-job", "office": "LRL"}]

    monkeypatch.setattr(batch_events, "get_scheduled_scripts_for_office", scripts)
    assert batch_events.get_scheduled_scripts(["SWT", "LRL"]) == [
        {"id": "lrl-job", "office": "LRL"}
    ]
    with pytest.raises(RuntimeError):
        batch_events.get_scheduled_scripts(["SWT"])
