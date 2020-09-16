from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

from synth.model import analysis


def clear_analysis_db(config):
    if database_exists(config.target):
        engine = create_engine(config.target)
        for table in reversed(analysis.metadata.sorted_tables):
            table.drop(engine)


def create_analysis_db(config):
    if not database_exists(config.target):
        create_database(config.target)

    engine = create_engine(config.target)
    analysis.Base.metadata.create_all(engine)
