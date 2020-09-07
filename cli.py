import subprocess

import click
import yaml


class Config:
    """
    Class encapsulating the configuration options that all the synth commands defined below need to
    run. Can be directly instantiated if you know what you're doing or can be created through the
    classmethod `create`.
    """

    @classmethod
    def create(cls, config_path):
        """
        Creates a new Config object using the YAML at the given path.

        :param config_path: the path to the config file
        :return: an instantiated Config object
        """
        with open(config_path, 'r') as f:
            return cls(**yaml.safe_load(f))

    def __init__(self, base, sources, target):
        """
        :param base: the base database URL for all source and target schemas
        :param sources: the names of the source schemas, must be in chronological order
        :param target: the name of the schema to output to
        """
        self.base = base
        self.sources = [f'{base}/{source}' for source in sources]
        self.target = f'{base}/{target}'


def setup(context, config_path):
    """
    Callback used on the abse synth command group which creates the Config object and attaches it
    to the context, replacing the obj attribute.

    :param context: the current context
    :param config_path: the config file's path
    """
    config = Config.create(config_path)
    context.obj = config


@click.group()
@click.option('-c', '--config', envvar='SYNTH_CONFIG', default='config.yml', show_default=True,
              callback=setup, expose_value=False, is_eager=True)
def synth():
    pass


@synth.command()
@click.option('-f', '--filename', default='rco_synthsys_live.py', help='output filename',
              show_default=True)
@click.pass_context
def generate(context, filename):
    """
    Generates a new SQLAlchemy model for the original Synthesys databases and outputs it to the
    given optional filename. The code is generated using sqlacodegen.
    """
    click.secho(f'Generating the model for source synth databases... ', fg='yellow', nl=False)

    with open(filename, 'w') as f:
        subprocess.call(['sqlacodegen', f'{context.obj.sources[-1]}'], stdout=f)

    click.secho(f'Done', fg='green')
