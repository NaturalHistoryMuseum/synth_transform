from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists, drop_database
from synth.model import analysis


def drop_analysis_db(config):
    if database_exists(config.target):
        drop_database(config.target)


def create_analysis_db(config):
    if not database_exists(config.target):
        create_database(config.target)

    engine = create_engine(config.target)
    analysis.Base.metadata.create_all(engine)
