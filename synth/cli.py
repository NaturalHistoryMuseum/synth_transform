from pathlib import Path

import click
import click_pathlib
import yaml

from synth.etl import etl_steps, GenerateSynthDatabaseModel
from synth.resources import RegisterResourcesStep
from synth.utils import Context, Config


def get_here():
    """
    Shortcut to get the Path for the directory where this script is.

    :return: the Path to the directory this script is in
    """
    return Path(__file__).parent


def setup(config_path):
    """
    Create a new Config object using the YAML file at the given path.

    :param config_path: the config file's path
    :return: a Config object
    """
    with open(config_path, 'r') as f:
        config = Config(**yaml.safe_load(f))

    context = Context(config)
    context.run_steps([
        RegisterResourcesStep(),
    ])
    return context


def setup_and_bind(context, config_path):
    """
    Sets the obj attribute on the given context with an instantiated Config object using the setup
    function above.

    :param context: the current context
    :param config_path: the config file's path
    """
    context.obj = setup(config_path)


@click.group()
@click.option('-c', '--config', envvar='SYNTH_CONFIG', callback=setup_and_bind, expose_value=False,
              is_eager=True, show_default='config.yml in project root', type=click_pathlib.Path(),
              default=lambda: get_here().parent / 'config.yml')
def synth():
    pass


@synth.command()
@click.option('-f', '--filename', default=lambda: get_here() / 'model' / 'rco_synthsys_live.py',
              help='output filename', show_default='synth/model/rco_synthsys_live.py',
              type=click_pathlib.Path())
@click.pass_obj
def generate(context, filename):
    """
    Generates a new SQLAlchemy model for the original Synthesys databases and outputs it to the
    given optional filename. The code is generated using sqlacodegen.
    """
    context.run_steps([GenerateSynthDatabaseModel(filename)])


@synth.command()
@click.option('--with-data/--without-data', default=True,
              help='Copy the data from the source to the target')
@click.pass_obj
def rebuild(context, with_data):
    """
    Drops the target database and then rebuilds it using the analysis model.
    """
    context.run_steps(etl_steps(with_data))


if __name__ == '__main__':
    # for dev!
    # generate(obj=setup(get_here().parent / 'config.yml'))
    # rebuild(obj=setup(get_here().parent / 'config.yml'))
    pass
