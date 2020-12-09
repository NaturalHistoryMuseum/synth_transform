# Synth Transform

![python versions](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8-blue)

_This project is under active development!_


## Overview


## Installation

### Virtualenv
Python 3.8 is recommended though currently the code should work in Python 3.6+.

It is recommended to use [virtualenv]() with this project to keep everything tidy:

```bash
virtualenv venv -p python3.8
```
(`venv` is in the .gitignore for this reason).

Then make sure you activate the environment:

```bash
source venv/bin/activate
```

### Setup
Once you've got your environment setup you can install the dependencies and CLI.
To do this run:

```bash
python setup.py develop
```

This will install the required dependencies and create the `synth` CLI program in your virtualenv's
`bin` directory.


## Config

Here's an example configuration file:

```yaml
---

# the database URLs of the source schemas, must be in chronological order
sources:
  - mysql+pymysql://USERNAME:PASSWORD@HOST/rco_synthesys_live
  - mysql+pymysql://USERNAME:PASSWORD@HOST/rco_synthesys2_live
  - mysql+pymysql://USERNAME:PASSWORD@HOST/rco_synthesys3_live
  - mysql+pymysql://USERNAME:PASSWORD@HOST/rco_synthesys4_live

# the database URL of the analysis schema
target: mysql+pymysql://USERNAME:PASSWORD@HOST/synth_analysis
```

## Usage

Make sure you've run `python setup.py develop` or `python setup.py install` in your
virtualenv/whatever and then you can use the `synth` command.

Help text available by running:

```bash
synth --help

Usage: synth [OPTIONS] COMMAND [ARGS]...

Options:
  -c, --config TEXT  [default: config.yml]
  --help             Show this message and exit.

Commands:
  generate  Generates a new SQLAlchemy model for the original Synthesys...
```

A config file is required to do anything.
There are 3 ways of providing the path to the YAML file;

  - don't do anything and it will be assumed that there is a file called `config.yml` in the
    current working directory
  - provide the file path using the `-c` or `--config` option, e.g. `synth -c config.yml`
  - provide the file path using the config environment variable `SYNTH_CONFIG`, e.g.
    `export SYNTH_CONFIG=config.yml`

### Commands
#### `generate`
```bash
synth generate
```

Generates a new SQLAlchemy model for the original Synthesys schemas and outputs it to the given
optional filename.
The code is generated using sqlacodegen.
The Synthesys 4 schema is used although they all have the same schema so it doesn't really matter!

#### `rebuild`
```bash
synth rebuild
```

Performs the actual transfer of data from the source Synthesys databases to the target analyitics
database.
To do this it drops and recreates all the tables in the analytics database and then runs a series of
steps to copy the data over, transforming it as it goes.

#### `update`
```bash
synth update
synth update --name <name>
synth update --name <name> --name <name> ...
```

Updates the supplementary data (if possible).
It is possible to specify which resources to update using the `--name` option which can be specified
multiple times to only update a set of names.

At present this command can take 20+ mins to run due to the
[DOI resource](https://github.com/NaturalHistoryMuseum/synth_transform/blob/main/synth/data/output_dois.json)
which is pulled from the Crossref API.

The names of the resources are listed below in the [_Supplementary Data_](#supplementary-data)
section.


#### `dump`
```bash
synth dump
synth dump --filename <filename>
```

Dumps the current analysis database to an SQL file.
The file includes table creation DDL and then data insert statements.


## Supplementary Data
In the `data` directory there are some supplementary files which are listed below along with their
source.

| file | name | source | updatable? |
| ---- | ---- | ------ | ---------- |
| `master_clean.json` | `institutions` | This file comes from [https://github.com/Vizzuality/Synthesys3/blob/master/Data/master_clean.json](https://github.com/Vizzuality/Synthesys3/blob/master/Data/master_clean.json) and provides a mapping between some dirty Synthesys place data and clean versions of places. | Yes, though only by pulling the latest version from GitHub which is unlikely to be updated at this point |
| `doi_metadata.db` | `doimetadata` | This database is generated using the Crossref API. We iterate over all of the DOIs we can find in the `NHM_Output` tables in the source synth databases, retrieve the DOI metadata from Crossref and then store it in this cache database. Updating this database can take 20+ mins. | Yes |
| `users.csv` | `users` | A CSV of PII safe user data which we can use to match users across the source synth databases. | No, only manually updatable |
| `access_request_rebuild.xlsx` | `accessrequestrebuild` | An Excel spreadsheet created by Sarah Vincent which aggregates together the data in the Facilities and Institutions tables from the source synth databases. | No, only manually updatable |
| `output_dois.db` | `dois` | This resource is a cached set of output IDs matched to DOIs using regexes, URLs, Crossref searches, and Refindit searches. This resource takes several hours to update, depending on throttling from Crossref and number of threads available for multiprocessing. | Yes |
