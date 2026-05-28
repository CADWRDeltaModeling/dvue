"""Tests for deterministic manager selection in `dvue ui`."""

from click.testing import CliRunner


def test_ui_defaults_to_registry_manager(monkeypatch):
    from dvue.cli import main
    from dvue.registry_ui import RegistryUIManager

    captured = {}

    def _fake_load_plugins():
        return []

    def _fake_serve_session_app(build_manager, title, port, crs):
        captured["manager_cls"] = type(build_manager())
        captured["title"] = title
        captured["crs"] = crs

    monkeypatch.setattr("dvue.registry.ReaderRegistry.load_plugins_from_entry_points", _fake_load_plugins)
    monkeypatch.setattr("dvue.session_persistence.serve_session_app", _fake_serve_session_app)

    HIST_DSS = r"d:\delta\dsm2_studies\timeseries\hist.dss"

    runner = CliRunner()
    result = runner.invoke(main, ["ui", HIST_DSS])

    assert result.exit_code == 0, result.output
    assert captured["manager_cls"] is RegistryUIManager
    assert captured["title"] == "dvue UI"


def test_ui_allows_explicit_plugin_manager_override(monkeypatch):
    from dvue.cli import main

    class _CustomManager:
        def __init__(self, files=(), **kwargs):
            self.files = list(files)

    class _FakePluginModule:
        DVueUIManager = _CustomManager

    captured = {}

    def _fake_load_plugins():
        return []

    def _fake_import_module(name):
        if name == "my_fake_plugin":
            return _FakePluginModule
        raise ImportError(name)

    def _fake_serve_session_app(build_manager, title, port, crs):
        captured["manager_cls"] = type(build_manager())

    monkeypatch.setattr("dvue.registry.ReaderRegistry.load_plugins_from_entry_points", _fake_load_plugins)
    monkeypatch.setattr("importlib.import_module", _fake_import_module)
    monkeypatch.setattr("dvue.session_persistence.serve_session_app", _fake_serve_session_app)

    HIST_DSS = r"d:\delta\dsm2_studies\timeseries\hist.dss"

    runner = CliRunner()
    result = runner.invoke(main, ["ui", "--plugin", "my_fake_plugin", HIST_DSS])

    assert result.exit_code == 0, result.output
    assert captured["manager_cls"] is _CustomManager
