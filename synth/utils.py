import abc
from contextlib import contextmanager
from datetime import datetime

import click
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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

        self.source_engines = [create_engine(source) for source in self.sources]
        self.target_engine = create_engine(self.target)


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

    def __init__(self, config):
        self.config = config

        self._source_sessions = [
            sessionmaker(bind=engine)() for engine in self.config.source_engines
        ]
        self._target_session_maker = sessionmaker(bind=self.config.target_engine)

    @property
    def synth1(self):
        """
        Returns a Session object for the synth 1 database. Note that these are created for each Step
        object and not shared between steps.

        :return: Session object
        """
        return self._source_sessions[0]

    @property
    def synth2(self):
        """
        Returns a Session object for the synth 2 database. Note that these are created for each Step
        object and not shared between steps.

        :return: Session object
        """
        return self._source_sessions[1]

    @property
    def synth3(self):
        """
        Returns a Session object for the synth 3 database. Note that these are created for each Step
        object and not shared between steps.

        :return: Session object
        """
        return self._source_sessions[2]

    @property
    def synth4(self):
        """
        Returns a Session object for the synth 4 database. Note that these are created for each Step
        object and not shared between steps.

        :return: Session object
        """
        return self._source_sessions[3]

    @contextmanager
    def target_session(self):
        """
        Context manager for a session on the target database. Yields an active, new session ready
        for use and once returned to, commits the session and then closes it. If any errors occur
        when committing or indeed during use, a rollback is issued and the exception is raised.

        :return: a Session object on the target database
        """
        session = self._target_session_maker()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

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
    def run(self):
        """
        Run the task.
        """
        pass

    def synth_sources(self):
        """
        Generator yielding the a 2-tuple made up of (number, session) for each synth source
        database, in order (i.e. (1, synth1 session), (2, synth2 session) ...).

        :return: a generator yielding 2-tuples made up of (number, session)
        """
        yield from enumerate(self._source_sessions, start=1)
