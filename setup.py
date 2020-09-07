from setuptools import setup


setup(
    name='synth',
    version='0.1',
    py_modules=['yourscript'],
    install_requires=[
        'click~=7.1.2',
        'click-pathlib~=2020.3.13.0',
        'mysqlclient~=2.0.1',
        'PyYAML~=5.3.1',
        'sqlacodegen~=2.3.0',
        'SQLAlchemy~=1.3.19',
    ],
    entry_points='''
        [console_scripts]
        synth=synth.cli:synth
    ''',
)

