from typing import cast

import click
import logging
from standalone_input.loaders import *
from standalone_input.loading_manager import LoadingManager
from standalone_input.mongo import MongoWrapper
from standalone_input.util import read_config

logging.root.setLevel(logging.INFO)
logging.root.addHandler(logging.StreamHandler())

logger = logging.getLogger(__name__)
_config = {}
_mongo = cast(MongoWrapper, None)


@click.group()
@click.option('--config', '-c', type=str, help='Path to config file')
@click.option('--no-mongo', '-s', is_flag=True, help='Don\'t use MongoDB, feed directly into Kafka')
def cli(config, no_mongo):
    global _config, _mongo
    _config = read_config(config)
    if not no_mongo:
        _mongo = MongoWrapper('loader_metadata', _config)


# ----- Domain list loading ----- #
@cli.command('load')
@click.argument('file', type=click.Path(exists=True), required=True)
@click.option('--category', '-t', type=str, help='The category field (if using the -d flag)', default='benign')
@click.option('--direct', '-d', is_flag=True,
              help='Load the file as a list of domain names, instead of interpreting it as a list of sources')
@click.option('--yes', '-y', is_flag=True, help='Don\'t interact, just start')
@click.option('--force', '-f', is_flag=True, help='Publish even previously seen domains for total re-collection')
def load(file, category, direct, yes, force):
    """Load sources from a file"""

    # ask user what type of file it is
    if yes:
        file_type = 'csv'
    else:
        file_type = click.prompt('File type', type=click.Choice(['csv', 'plain']), default='csv')
    # confirm with user before importing
    if not yes:
        if not click.confirm(f"Load domain list(s) from {file}?", default=True):
            return
    else:
        logger.info(f"Importing sources from {file}")
    # load sources from file
    click.echo(f'Loading sources from {file} ({file_type})...')
    if direct:
        loader = DirectLoader(file, category)
    else:
        loader = SourceLoader()
        if file_type == 'csv':
            loader.source_csv(file, column=1, category=5, category_source=6, getter=7, mapper=8)
        elif file_type == 'plain':
            loader.source_plain(file)
        click.echo(f'Found {loader.source_count()} sources')

    try:
        manager = LoadingManager(_mongo, _config)
        manager.load_from_loader(loader, force)
    except ValueError as e:
        if 'unknown url type' in str(e):
            click.echo('Can\'t download. File is probably a domain list. Try again with --direct or -d.', err=True)
        else:
            click.echo(str(e), err=True)


@cli.command('load-misp')
@click.argument('feed', type=str, required=True)
@click.option('--force', '-f', is_flag=True, help='Publish even previously seen domains for total re-collection')
def load_misp(feed, force):
    """Load domains from MISP feed defined in config and selected by FEED name"""
    try:
        loader = MISPLoader(feed, _config)
        manager = LoadingManager(_mongo, _config)
        manager.load_from_loader(loader, force)
    except ValueError as e:
        click.echo(str(e), err=True)


if __name__ == '__main__':
    cli()
