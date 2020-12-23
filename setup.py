from setuptools import setup

setup(
    name='synth',
    version='0.1',
    py_modules=['synth'],
    install_requires=[
        'click~=7.1.2',
        'click-pathlib~=2020.3.13.0',
        'crossrefapi~=1.5.0',
        'geonamescache==1.2.0',
        'pandas~=1.1.4',
        'pycountry~=20.7.3',
        'pymysql~=0.10.0',
        'pymysql[rsa]~=0.10.0',
        'PyYAML~=5.3.1',
        'seaborn~=0.11.0',
        'sqlacodegen~=2.3.0',
        'SQLAlchemy~=1.3.19',
        'sqlalchemy-utils~=0.36.8',
        'beautifulsoup4~=4.9.1',
        'lxml~=4.5.2',
        'fuzzywuzzy~=0.18.0',
        'python-Levenshtein~=0.12.0',
        'sqlitedict~=1.7.0',
        'unidecode~=1.1.1',
        'untangle~=1.1.1',
        'tqdm~=4.50.0',
        'mendeley~=0.3.2',
        'requests>=2.11.1',
        'xlrd~=1.2.0',
    ],
    entry_points='''
        [console_scripts]
        synth=synth.cli:synth
    ''',
)
