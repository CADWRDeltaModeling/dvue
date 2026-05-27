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


@main.command(name="list-plugins")
def list_plugins():
    """List all available dvue plugins (from entry points and registry).

    Displays plugins discovered via setuptools entry points (``dvue.plugins``
    group) and any readers currently registered in the :class:`~dvue.registry.ReaderRegistry`.
    """
    from dvue.registry import ReaderRegistry

    # Auto-load plugins from entry points
    loaded = ReaderRegistry.load_plugins_from_entry_points()

    if loaded:
        click.echo("Loaded plugins (from entry points):")
        for name in loaded:
            click.echo(f"  • {name}")
        click.echo()

    # Show registered readers
    readers = ReaderRegistry.get_registered_readers()
    extensions = ReaderRegistry.get_registered_extensions()

    if readers:
        click.echo(f"Registered readers ({len(readers)}):")
        for ref_type, reader_cls in sorted(readers.items()):
            exts_for_type = [e for e, c in extensions.items() if c is reader_cls]
            if exts_for_type:
                ext_str = ", ".join(sorted(exts_for_type))
                click.echo(f"  • {ref_type:25} → {reader_cls.__name__:30} ({ext_str})")
            else:
                click.echo(f"  • {ref_type:25} → {reader_cls.__name__:30} (no extensions)")
    else:
        click.echo("No readers registered.")


# ---------------------------------------------------------------------------
# diagnose command — plugin and registry diagnostics
# ---------------------------------------------------------------------------

@main.command(name="diagnose")
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Show detailed error messages and tracebacks.",
)
def diagnose(verbose):
    """Diagnose dvue plugin and registry status.

    Checks:
    1. Entry points discovery (``dvue.plugins`` group)
    2. Plugin import and registration success/failure
    3. Registered readers and file extension mappings
    4. Version information

    Useful for troubleshooting "no registered reader" errors when files won't load.
    """
    import sys
    from dvue.registry import ReaderRegistry

    click.echo("=" * 70)
    click.echo("DVue Plugin & Registry Diagnostics")
    click.echo("=" * 70)

    # Step 1: Check entry points discovery
    click.echo("\n[1] Entry Points Discovery")
    click.echo("-" * 70)
    try:
        from importlib.metadata import entry_points

        eps = entry_points()
        group = (
            eps.select(group="dvue.plugins")
            if hasattr(eps, "select")
            else eps.get("dvue.plugins", [])
        )
        ep_count = len(group)
        click.echo(f"✓ Entry points API available")
        click.echo(f"  Found {ep_count} plugin(s) in dvue.plugins group:")

        if ep_count > 0:
            for ep in group:
                click.echo(f"    • {ep.name:25} = {ep.value}")
        else:
            click.echo("    (none)")

    except Exception as e:
        click.echo(f"✗ Failed to query entry points: {e}")
        if verbose:
            import traceback

            traceback.print_exc()

    # Step 2: Load plugins via registry
    click.echo("\n[2] Plugin Loading")
    click.echo("-" * 70)
    try:
        loaded = ReaderRegistry.load_plugins_from_entry_points()
        if loaded:
            click.echo(f"✓ Loaded {len(loaded)} plugin(s):")
            for name in loaded:
                click.echo(f"    • {name}")
        else:
            click.echo("✓ No plugins to load (dvue.plugins group is empty)")

    except Exception as e:
        click.echo(f"✗ Failed to load plugins: {e}")
        if verbose:
            import traceback

            traceback.print_exc()

    # Step 3: Check registered readers
    click.echo("\n[3] Registered Readers & Extensions")
    click.echo("-" * 70)
    try:
        readers = ReaderRegistry.get_registered_readers()
        extensions = ReaderRegistry.get_registered_extensions()

        click.echo(f"✓ {len(readers)} reader(s) registered, {len(extensions)} extension(s) mapped:")

        if readers:
            for ref_type in sorted(readers.keys()):
                reader_cls = readers[ref_type]
                exts_for_type = [e for e, c in extensions.items() if c is reader_cls]
                if exts_for_type:
                    ext_str = ", ".join(sorted(exts_for_type))
                    click.echo(
                        f"    • {ref_type:25} → {reader_cls.__name__:30} ({ext_str})"
                    )
                else:
                    click.echo(
                        f"    • {ref_type:25} → {reader_cls.__name__:30} (no file ext)"
                    )
        else:
            click.echo("    (none)")

    except Exception as e:
        click.echo(f"✗ Failed to query registered readers: {e}")
        if verbose:
            import traceback

            traceback.print_exc()

    # Step 4: Environment info
    click.echo("\n[4] Environment Info")
    click.echo("-" * 70)
    try:
        import dvue

        click.echo(f"✓ dvue version:  {dvue.__version__}")
        click.echo(f"  Python:        {sys.version.split()[0]}")
        click.echo(f"  Executable:    {sys.executable}")

    except Exception as e:
        click.echo(f"✗ Failed to get version info: {e}")

    # Summary
    click.echo("\n" + "=" * 70)
    if extensions:
        click.echo(
            "✓ dvue is ready! Extensions are mapped. Files should load correctly."
        )
    else:
        click.echo(
            "⚠ WARNING: No file extensions are registered. "
            "Files may not load. Run with -v for details."
        )
    click.echo("=" * 70)


main.add_command(show_version)
main.add_command(list_plugins)
main.add_command(diagnose)


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
        "Python module to import before launching the UI (optional). "
        "Modules that call ReaderRegistry.register() at import time register readers. "
        "Note: plugins from entry points (dvue.plugins group) are loaded automatically. "
        "Use this flag for development or to load local/custom modules. "
        "May be specified multiple times."
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

    **Plugin Discovery**

    Plugins are loaded in this order:

    1. **Entry points** — all plugins registered in the ``dvue.plugins`` entry
       point group are auto-discovered and loaded automatically. This happens
       without any CLI flags required.
    2. **Explicit --plugin flags** — modules specified via ``--plugin MODULE``
       are imported in order.  These may override extension mappings from
       entry-point plugins (last-write-wins).

    **Examples**

    Drag-and-drop any file whose extension is registered by an installed plugin::

        dvue ui                                      # empty start
        dvue ui file.h5 file.dss                     # pre-load files
        dvue ui --plugin my_custom.readers file.xyz # load custom module

    With entry points configured, extensions are available immediately without
    needing ``--plugin`` flags::

        # If dsm2ui is installed with entry point, .h5/.dss readers auto-load
        dvue ui run.h5 hist_qual.dss

    See :meth:`dvue.registry.ReaderRegistry.load_plugins_from_entry_points` for
    details on the entry point discovery mechanism.
    """
    import importlib
    from dvue.registry import ReaderRegistry

    # Auto-load plugins from entry points (dvue.plugins group)
    auto_loaded = ReaderRegistry.load_plugins_from_entry_points()

    # Then load any explicit --plugin CLI args
    plugin_modules = []
    for module_name in plugins:
        try:
            plugin_modules.append(importlib.import_module(module_name))
        except ImportError as exc:
            raise click.ClickException(
                f"Could not import plugin module {module_name!r}: {exc}"
            ) from exc

    from dvue.registry_ui import RegistryUIManager
    from dvue.session_persistence import serve_session_app, serve_desktop_app

    manager_cls = RegistryUIManager
    effective_crs = None
    for mod in plugin_modules:
        # Optional plugin hook: module-level manager override.
        # Last plugin wins when multiple plugins provide this symbol.
        if hasattr(mod, "DVueUIManager"):
            manager_cls = getattr(mod, "DVueUIManager")
        # Optional plugin hook: module-level map CRS override.
        if hasattr(mod, "DVueUI_CRS"):
            effective_crs = getattr(mod, "DVueUI_CRS")

    file_list = list(files)

    def build_manager():
        return manager_cls(files=file_list)

    _serve = serve_desktop_app if desktop else serve_session_app
    _serve(build_manager, title="dvue UI", port=port, crs=effective_crs)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
