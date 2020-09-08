import subprocess
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import click
import click_pathlib
import yaml

from synth import etl


class Config:
    """
    Class encapsulating the configuration options that all the synth commands defined below need to
    run.
    """

    def __init__(self, sources, target):
        """
        :param sources: the database URLs of the source schemas, must be in chronological order
        :param target: the database URL of the schema to output to
        """
        self.sources = sources
        self.target = target


def setup(config_path=None):
    """
    Create a new Config object using the YAML file at the given path. If no path is provided then
    the config.yml file is looked for in the parent directory to the location of this script.

    :param config_path: the config file's path
    :return: a Config object
    """
    if config_path is None:
        config_path = get_here().parent / 'config.yml'

    with open(config_path, 'r') as f:
        return Config(**yaml.safe_load(f))


def setup_and_bind(context, config_path):
    """
    Sets the obj attribute on the given context with an instantiated Config object using the setup
    function above.

    :param context: the current context
    :param config_path: the config file's path
    """
    context.obj = setup(config_path)


def get_here():
    """
    Shortcut to get the Path for the directory where this script is.

    :return: the Path to the directory this script is in
    """
    return Path(__file__).parent


@contextmanager
def task(message, done='Done', time=True):
    """
    Handy context manager for printing basic info about the start and end of a task. The message
    passed is printed in yellow first with "... " appended and then the context manager yields. When
    the context collapses the passed done message is printed in green.

    :param message: the task message
    :param done: the done message
    :param time: whether to time the execution and print it as part of the done message
    """
    click.secho(f'{message}... ', fg='yellow', nl=False)
    start = datetime.now()
    yield
    end = datetime.now()
    if time:
        done = f'{done} [took {end - start}]'
    click.secho(done, fg='green')


@click.group()
@click.option('-c', '--config', envvar='SYNTH_CONFIG', callback=setup_and_bind, expose_value=False,
              is_eager=True, show_default='config.yml in project root', type=click_pathlib.Path())
def synth():
    pass


@synth.command()
@click.option('-f', '--filename', default=lambda: get_here() / 'model' / 'rco_synthsys_live.py',
              help='output filename', show_default='synth/model/rco_synthsys_live.py',
              type=click_pathlib.Path())
@click.pass_obj
def generate(config, filename):
    """
    Generates a new SQLAlchemy model for the original Synthesys databases and outputs it to the
    given optional filename. The code is generated using sqlacodegen.
    """
    with task('Generating the model for source synth databases'):
        with open(filename, 'w') as f:
            subprocess.call(['sqlacodegen', f'{config.sources[-1]}'], stdout=f)


@synth.command()
@click.pass_obj
def rebuild(config):
    """
    Drops the target database and then rebuilds it using the analysis model.
    """
    with task('Dropping existing database target if necessary'):
        etl.drop_analysis_db(config)
    with task('Creating new target database using model'):
        etl.create_analysis_db(config)


if __name__ == '__main__':
    # for dev!
    # generate(obj=setup())
    # rebuild(obj=setup())
    pass
