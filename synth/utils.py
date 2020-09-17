import abc
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from enum import Enum

import click
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class SynthRound(Enum):
    """
    Enum representing the 4 synth rounds.
    """
    ONE = 1
    TWO = 2
    THREE = 3
    FOUR = 4


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


def setup(config_path):
    """
    Create a new Config object using the YAML file at the given path.

    :param config_path: the config file's path
    :return: a Config object
    """
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


class Step(abc.ABC):
    """
    This class represents a step in the ETL process.
    """

    def __init__(self, context):
        self.context = context

    @property
    @abc.abstractmethod
    def message(self):
        """
        This is the message used for the task output, indicating to users what is currently
        happening.

        :return: a string message (this will be passed directly to the task context manager above
        """
        pass

    def run(self):
        """
        Runs the step.
        """
        source_sessions = [sessionmaker(bind=engine)() for engine in self.context.source_engines]
        target_session = sessionmaker(bind=self.context.target_engine)()

        with task(self.message):
            try:
                self._run(target_session, *source_sessions)
                target_session.commit()
            except Exception as e:
                target_session.rollback()
                raise e
            finally:
                for source_session in source_sessions:
                    source_session.close()
                target_session.close()

    @abc.abstractmethod
    def _run(self, target, synth1, synth2, synth3, synth4):
        """
        Abstract function to be overwritten with actual step logic.

        :param target: a Session object for the target database
        :param synth1: a Session object for the synth 1 database
        :param synth2: a Session object for the synth 2 database
        :param synth3: a Session object for the synth 3 database
        :param synth4: a Session object for the synth 4 database
        """
        pass


class Context:
    """
    One object of this type should be passed around to all the steps in the run to store global
    state.
    """

    def __init__(self, config):
        self.config = config
        self.source_engines = [create_engine(source) for source in self.config.sources]
        self.target_engine = create_engine(self.config.target)
        self.mappings = defaultdict(dict)

    def mapping_set(self, source_table, original, new, synth=None):
        """
        Add a mapping from one value to another. The original value should come from the given
        source_table (and the optional synth round) and map to the new value from the analysis
        target table.

        :param source_table: the table the original value comes from
        :param original: the value from the original source table
        :param new: the new value the original value maps to
        :param synth: the synth round the original value comes from (optional, defaults to None)
        """
        self.mappings[source_table][(synth, original)] = new

    def mapping_get(self, source_table, original, synth=None, default=None):
        """
        Retrieves the new value mapped to the provided original value in the given source_table and
        optional synth round.

        :param source_table: the table the original value comes from
        :param original: the value from the original source table
        :param synth: the synth round the original value comes from (optional, defaults to None)
        :param default: the default value to return if the original value has no mapping
        :return: the new value that the original value maps to or the default if no mapping is found
        """
        return self.mappings[source_table].get((synth, original), default)
