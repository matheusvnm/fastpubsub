import importlib
import sys

import pytest

MODULE_NAME = "docs.snippets.basic_usage.e8_02_deployment_config"


def _import_fresh():
    sys.modules.pop(MODULE_NAME, None)
    return importlib.import_module(MODULE_NAME)


class TestDeploymentConfigSnippet:
    @pytest.mark.docs
    def test_import_succeeds_when_gcp_project_id_set(self, monkeypatch):
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project-id")

        module = _import_fresh()

        assert module.PROJECT_ID == "test-project-id"
        assert module.broker is not None
        assert module.app is not None
        assert getattr(module.broker, "project_id", None) == "test-project-id"

    @pytest.mark.docs
    def test_import_raises_when_gcp_project_id_missing(self, monkeypatch):
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        sys.modules.pop(MODULE_NAME, None)

        with pytest.raises(
            RuntimeError,
            match=r"GCP_PROJECT_ID environment variable not set\.",
        ):
            importlib.import_module(MODULE_NAME)

        sys.modules.pop(MODULE_NAME, None)
