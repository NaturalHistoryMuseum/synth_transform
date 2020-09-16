from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

from synth.model import analysis
from synth.utils import Step


def get_steps(config, with_data=True):
    """
    Returns the ETL steps. If the with_data flag is passed as False then the data transform are
    omitted and the tables are simply dropped and recreated.

    :param config: the Config object
    :param with_data: whether to transfer the data over too (default: True)
    :return: a list of ordered steps to perform the requested ETL
    """
    steps = [
        ClearAnalysisDB(config),
        CreateAnalysisDB(config),
    ]
    if with_data:
        pass
        # steps.append(more)

    return steps


class ClearAnalysisDB(Step):
    """
    This step drops all tables from the analysis database, if there is one.
    """

    @property
    def message(self):
        return 'Dropping tables in target database (if necessary)'

    def run(self):
        if database_exists(self.config.target):
            engine = create_engine(self.config.target)
            analysis.Base.metadata.drop_all(engine)


class CreateAnalysisDB(Step):
    """
    This step creates all tables for the analysis database, if the database doesn't already exist.
    """

    @property
    def message(self):
        return 'Creating new target database using model (if necessary)'

    def run(self):
        if not database_exists(self.config.target):
            create_database(self.config.target)
        engine = create_engine(self.config.target)
        analysis.Base.metadata.create_all(engine)


