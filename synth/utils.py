import abc
import enum
import re
import warnings
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime

import click
from bs4 import MarkupResemblesLocatorWarning, BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from synth.model.analysis import Call


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

    def __init__(self, sources, target, resources=None):
        """
        :param sources: the database URLs of the source schemas, must be in chronological order
        :param target: the database URL of the schema to output to
        """
        self.sources = sources
        self.target = target
        self._resources = resources or {}

    def resource_opt(self, config_option, default=None):
        """
        Convenience method to retrieve an option that may or may not be defined.
        :param config_option: a dot-delimited string describing the option, e.g. "doi.refresh"
        """
        config_value = self._resources
        levels = config_option.split('.')
        for i, k in enumerate(levels):
            config_value = config_value.get(k, {} if i < len(levels) - 1 else default)
        return config_value


def find_names(author_string):
    """
    Attempts to find names in the given string using regexes. Usually only finds family names, as most names have been
    specified as (e.g.) "FamilyName, A. B.".
    """
    and_regex = re.compile(r'( and |&)', re.I)
    name_regex = re.compile(r"([^\W\d_]{3,}[-' ]?)+")
    if and_regex.search(author_string) is not None:
        author_string = and_regex.sub('; ', author_string)
    names = name_regex.findall(author_string)
    return names


def clean_string(string):
    """
    Attempts to remove HTML and unwanted characters from strings.
    """
    unwanted_chars_rgx = re.compile(r'[\r\n\t]+')
    alphanum = re.compile(r'\w')
    multi_space_rgx = re.compile(r'\s{2,}')
    start_space_rgx = re.compile(r'^\s+')
    text = unwanted_chars_rgx.sub(' ', string)
    if len(text) == 0:
        return
    with warnings.catch_warnings():
        # otherwise we get bs4 warnings when string doesn't have HTML in it
        warnings.simplefilter('ignore', category=MarkupResemblesLocatorWarning)
        soup = BeautifulSoup(text, 'lxml').text.replace('\xa0', ' ')
    if alphanum.search(soup) is None:
        return
    else:
        return start_space_rgx.sub('', multi_space_rgx.sub(' ', soup))


def to_datetime(value, date_format='%a %b %d %H:%M:%S %Z %Y'):
    """
    Converts the given string to a datetime and returns it. If the value passed is falsey then None
    is returned, otherwise this is exactly the same as calling datetime.strptime(value, date_format)
    directly.

    :param value: the date as a string
    :param date_format: the format the date is in
    :return: a datetime or None
    """
    if not value:
        return None
    return datetime.strptime(value, date_format)


def clean_institution(lookup, institution):
    """
    Given the institutions resource and an institution name, clean up the passed name and attempt
    to match it to the lookup in case we have a cleaner version.

    If no cleaner version exists in the lookup then the name (with html bits removed) is returned.

    :param lookup: the data of Institutions resource (should be a dict)
    :param institution: the candidate institution name
    :return: the cleaned version of the name
    """
    if institution is None:
        return None

    institution = clean_string(institution)
    if institution in lookup:
        match = lookup[institution]
        return None if match == 'nil' else match

    return institution


def get_synth_round(call_id, target):
    """
    Given a Call ID, return the associated SynthRound enum.

    :param call_id: the Call ID
    :param target: the target database to query (should be the analysis database)
    :return: an SynthRound enum value
    """
    call = target.query(Call).filter(Call.id == call_id).one()
    return SynthRound(call.round_id)


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

    def map(self, source_table, original, new, synth_round=None):
        """
        Add a mapping from one value to another. The original value should come from the given
        source_table (and the optional synth round) and map to the new value from the analysis
        target table.

        If the synth_round parameter is None then the mapping will be made for all synth rounds.

        :param source_table: the table the original value comes from
        :param original: the value from the original source table
        :param new: the new value the original value maps to
        :param synth_round: the synth round the original value is from (optional, defaults to None)
        """
        rounds = list(SynthRound) if synth_round is None else [synth_round]
        for sr in rounds:
            self.mappings[source_table][(sr, original)] = new

    def translate(self, source_table, original, synth_round, default=None):
        """
        Retrieves the new value mapped to the provided original value in the given source_table and
        synth round.

        :param source_table: the table the original value comes from
        :param original: the value from the original source table
        :param synth_round: the synth round the original value is from
        :param default: the default value to return if the original value has no mapping
        :return: the new value that the original value maps to or the default if no mapping is found
        """
        return self.mappings[source_table].get((synth_round, original), default)

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
