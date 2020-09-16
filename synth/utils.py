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

        self._target_engine = create_engine(self.target)
        self._target_session = sessionmaker(bind=self._target_engine)

    @contextmanager
    def target_session(self):
        session = self._target_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


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
