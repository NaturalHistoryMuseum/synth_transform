from sqlalchemy import create_engine, func
from sqlalchemy_utils import create_database, database_exists

from synth.model import analysis
from synth.model.analysis import Round, Call
from synth.model.rco_synthsys_live import t_NHM_Call
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
        steps.extend([
            FillRoundTable(config),
            FillCallTable(config),
        ])

    return steps


class ClearAnalysisDB(Step):
    """
    This step drops all tables from the analysis database, if there is one.
    """

    @property
    def message(self):
        return 'Drop tables in target database (if necessary)'

    def _run(self, *args, **kwargs):
        if database_exists(self.config.target):
            engine = create_engine(self.config.target)
            analysis.Base.metadata.drop_all(engine)


class CreateAnalysisDB(Step):
    """
    This step creates all tables for the analysis database, if the database doesn't already exist.
    """

    @property
    def message(self):
        return 'Create new target database using model (if necessary)'

    def _run(self, *args, **kwargs):
        if not database_exists(self.config.target):
            create_database(self.config.target)
        engine = create_engine(self.config.target)
        analysis.Base.metadata.create_all(engine)


class FillRoundTable(Step):
    """
    Fills the Round table with the synth round data.
    """

    @property
    def message(self):
        return 'Fill round table with data'

    def _run(self, target, *synth_sources):
        """
        Fill the Round table with data from the NHM_Call tables in each of the synth sources.
        Notes:
            - we force the ids of each Round to match the synth round for ease of use elsewhere
        """
        for synth_round, source in enumerate(synth_sources, start=1):
            # find the minimum call open time on this db
            start = source.query(func.min(t_NHM_Call.c.dateOpen)).scalar()
            # and the maximum call close time on this db
            end = source.query(func.max(t_NHM_Call.c.dateClosed)).scalar()
            # then create a new Round object in the target session
            target.add(Round(id=synth_round, name=f'Synthesys {synth_round}', start=start, end=end))


class FillCallTable(Step):

    @property
    def message(self):
        return 'Fill Call table with data'

    def _run(self, target, *synth_sources):
        """
        Fill the Call table with data from the NHM_Call tables in each of the synth sources.
        Notes:
            - the Call ids are generated using an offset to make it easier to map them in other
              places. The offset is calculated like so: (offset * synth_round) + NHM_Call.callID.
        """
        offset = 100
        for synth_round, source in enumerate(synth_sources, start=1):
            # TODO: is the call column ordered correctly? Should we order on date instead? Does it
            #       even matter?
            for call in source.query(t_NHM_Call).order_by(t_NHM_Call.c.call.asc()):
                # TODO: do we want to use the call.callID or start from 1?
                call_id = (offset * synth_round) + call.callID
                target.add(Call(id=call_id, round=synth_round, start=call.dateOpen,
                                end=call.dateClosed))
