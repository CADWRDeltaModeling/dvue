# -*- coding: utf-8 -*-
"""Console script for pydelmod."""
import sys
import click
import panel as pn

pn.extension()


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
def main():
    pass

@click.command()
def show_version():
    import dvue
    click.echo(dvue.__version__)

main.add_command(show_version)

if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
