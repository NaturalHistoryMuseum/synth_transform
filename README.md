# Synth Transform

_This project is under active development!_


## Overview


## Installation

### Virtualenv
It is recommended to use [virtualenv]() with this project to keep everything tidy:

```bash
virtualenv venv -p python3.8
```
(`venv` is in the .gitignore for this reason).

Then make sure you activate the environment:

```bash
source venv/bin/activate
```


### MySQL Python Driver
The MySQL Python driver we're using is `mysqlclient` which requires some additional OS libraries
cause it uses a MySQL C library under the hood.
Check out [the pip page](https://pypi.org/project/mysqlclient/) for some support.

_If this library is an issue to install on Windows we may need to use
[PyMySQL](https://pypi.org/project/PyMySQL/)._


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

# base database URL, must not end in a slash!
base: mysql://USERNAME:PASSWORD@HOST

# the names of the source schemas, must be in chronological order
sources:
  - rco_synthesys_live
  - rco_synthesys2_live
  - rco_synthesys3_live
  - rco_synthesys4_live

# the name of the analysis schema
target: synth_analysis
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


## Supplementary Data
In the `data` directory there are some supplementary files which are listed below along with their
source.

| file | source |
| ---- | ------ |
| `master_clean.json` | This file comes from [https://github.com/Vizzuality/Synthesys3/blob/master/Data/master_clean.json](https://github.com/Vizzuality/Synthesys3/blob/master/Data/master_clean.json) and provides a mapping between some dirty Synthesys place data and clean versions of places. |
