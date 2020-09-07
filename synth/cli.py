import subprocess
from pathlib import Path

import click
import yaml


class Config:
    """
    Class encapsulating the configuration options that all the synth commands defined below need to
    run.
    """

    @classmethod
    def create(cls, config_path):
        """
        Creates a new Config object using the YAML at the given path.

        :param config_path: the path to the config file
        :return: an instantiated Config object
        """

    def __init__(self, base, sources, target):
        """
        :param base: the base database URL for all source and target schemas
        :param sources: the names of the source schemas, must be in chronological order
        :param target: the name of the schema to output to
        """
        self.base = base
        self.sources = [f'{base}/{source}' for source in sources]
        self.target = f'{base}/{target}'


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


@click.group()
@click.option('-c', '--config', envvar='SYNTH_CONFIG', callback=setup_and_bind, expose_value=False,
              is_eager=True, show_default='config.yml in project root', type=click.Path())
def synth():
    pass


@synth.command()
@click.option('-f', '--filename', default=lambda: get_here() / 'model' / 'rco_synthsys_live.py',
              help='output filename', show_default='synth/model/rco_synthsys_live.py',
              type=click.Path())
@click.pass_obj
def generate(config, filename):
    """
    Generates a new SQLAlchemy model for the original Synthesys databases and outputs it to the
    given optional filename. The code is generated using sqlacodegen.
    """
    filename = Path(filename)
    click.secho(f'Generating the model for source synth databases... ', fg='yellow', nl=False)
    with open(filename, 'w') as f:
        subprocess.call(['sqlacodegen', f'{config.sources[-1]}'], stdout=f)
    click.secho(f'Done', fg='green')


if __name__ == '__main__':
    # for dev!
    generate(obj=setup())
