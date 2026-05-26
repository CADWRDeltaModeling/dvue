# -*- coding: utf-8 -*-
"""Console script for dvue."""
import sys
import click


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
def main():
    pass


@click.command()
def show_version():
    import dvue

    click.echo(dvue.__version__)


main.add_command(show_version)


# ---------------------------------------------------------------------------
# ui command — generic registry-backed file viewer
# ---------------------------------------------------------------------------

@main.command(name="ui")
@click.argument("files", nargs=-1, type=click.Path(dir_okay=False, readable=True))
@click.option(
    "--plugin",
    "plugins",
    multiple=True,
    metavar="MODULE",
    help=(
        "Python module to import before launching the UI.  Importing a module "
        "that calls ReaderRegistry.register() at module level registers its "
        "readers so that the corresponding file extensions are supported.  "
        "May be specified multiple times.  Example: "
        "--plugin dsm2ui.dsm2ui --plugin schismviz.readers"
    ),
)
@click.option(
    "--port",
    default=0,
    show_default=True,
    type=int,
    help="Port for the web server (0 = random available port).",
)
@click.option(
    "--desktop",
    is_flag=True,
    default=False,
    help="Open in a native desktop window (requires pywebview).",
)
def ui_command(files, plugins, port, desktop):
    """Generic file viewer — drag-and-drop any registered file type.

    Launches :class:`~dvue.registry_ui.RegistryUIManager` with FILES
    pre-loaded.  Omit FILES to start empty and add files via drag-and-drop.

    Reader plugins are loaded by passing ``--plugin MODULE``.  Any module
    that calls ``ReaderRegistry.register()`` at import time is a valid
    plugin.  Install the package first, then reference its reader module::

        dvue ui --plugin dsm2ui.dsm2ui run.h5 hist_qual.dss
        dvue ui --plugin schismviz.readers output.staout
        dvue ui --plugin dsm2ui.dsm2ui --plugin schismviz.readers --desktop
        dvue ui           # empty start, drag-and-drop files in

    \b
    Supported extensions are determined entirely by what has been registered
    via --plugin imports.  Without any --plugin the catalog starts empty and
    only accepts files whose readers were registered by other installed
    packages at import time.
    """
    import importlib

    for module_name in plugins:
        try:
            importlib.import_module(module_name)
        except ImportError as exc:
            raise click.ClickException(
                f"Could not import plugin module {module_name!r}: {exc}"
            ) from exc

    from dvue.registry_ui import RegistryUIManager
    from dvue.session_persistence import serve_session_app, serve_desktop_app

    file_list = list(files)

    def build_manager():
        return RegistryUIManager(files=file_list)

    _serve = serve_desktop_app if desktop else serve_session_app
    _serve(build_manager, title="dvue UI", port=port)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
