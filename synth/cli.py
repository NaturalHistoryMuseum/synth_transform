import subprocess
from pathlib import Path

import click
import click_pathlib

from synth.etl import get_steps
from synth.utils import setup_and_bind, task


def get_here():
    """
    Shortcut to get the Path for the directory where this script is.

    :return: the Path to the directory this script is in
    """
    return Path(__file__).parent


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
def generate(config, filename):
    """
    Generates a new SQLAlchemy model for the original Synthesys databases and outputs it to the
    given optional filename. The code is generated using sqlacodegen.
    """
    with task('Generating the model for source synth databases'):
        with open(filename, 'w') as f:
            subprocess.call(['sqlacodegen', f'{config.sources[-1]}'], stdout=f)


@synth.command()
@click.option('--with-data/--without-data', default=True,
              help='Copy the data from the source to the target')
@click.pass_obj
def rebuild(config, with_data):
    """
    Drops the target database and then rebuilds it using the analysis model.
    """
    for step in get_steps(config, with_data):
        with task(step.message):
            step.run()


if __name__ == '__main__':
    # for dev!
    # from synth.utils import setup
    # config_path = get_here().parent / 'config.yml'
    # generate(obj=setup(config_path))
    # rebuild(obj=setup(config_path))
    pass
