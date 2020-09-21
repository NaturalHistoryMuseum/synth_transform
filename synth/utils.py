import abc
import enum
import re
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@enum.unique
class SynthRound(enum.IntEnum):
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


def find_doi(string):
    """
    Attempts to find a DOI in the given string using a regex.

    :param string: a string to search
    :return: the DOI string or None if nothing was found
    """
    # regex source: https://www.crossref.org/blog/dois-and-matching-regular-expressions/
    doi_regex = re.compile(r'10.\d{4,9}/[-._;()/:A-Z0-9]+', re.I)
    doi_match = doi_regex.search(string)
    if doi_match:
        return doi_match.group()


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

    @property
    @abc.abstractmethod
    def message(self):
        """
        This is the message used for the task output, indicating to users what is currently
        happening.

        :return: a string message (this will be passed directly to the task context manager above
        """
        pass

    @abc.abstractmethod
    def run(self, context, target, synth1, synth2, synth3, synth4):
        """
        Abstract function to be overwritten with actual step logic.

        :param context: a Context object
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
        self.resources = {}

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

    def run_steps(self, steps):
        """
        Runs all the given steps, providing each one with the required run parameters and cleaning
        up after each.

        :param steps: the steps to run
        """
        for step in steps:
            # create a new set of sessions for the source databases
            source_sessions = [sessionmaker(bind=engine)() for engine in self.source_engines]
            # and a new target database session
            target_session = sessionmaker(bind=self.target_engine)()

            with task(step.message):
                try:
                    step.run(self, target_session, *source_sessions)
                    target_session.commit()
                except Exception as e:
                    target_session.rollback()
                    raise e
                finally:
                    for source_session in source_sessions:
                        source_session.close()
                    target_session.close()
