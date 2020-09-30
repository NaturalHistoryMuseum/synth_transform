from setuptools import setup

setup(
    name='synth',
    version='0.1',
    py_modules=['synth'],
    install_requires=[
        'click~=7.1.2',
        'click-pathlib~=2020.3.13.0',
        'crossrefapi~=1.5.0',
        'pycountry~=20.7.3',
        'pymysql~=0.10.0',
        'pymysql[rsa]~=0.10.0',
        'PyYAML~=5.3.1',
        'requests>=2.11.1',
        'sqlacodegen~=2.3.0',
        'SQLAlchemy~=1.3.19',
        'sqlalchemy-utils~=0.36.8',
        'beautifulsoup4',
        'lxml'
    ],
    entry_points='''
        [console_scripts]
        synth=synth.cli:synth
    ''',
)
