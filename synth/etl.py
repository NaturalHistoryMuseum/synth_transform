import itertools

import pycountry
from sqlalchemy import create_engine, func
from sqlalchemy_utils import create_database, database_exists

from synth.errors import SpecificDisciplineParentMismatch
from synth.model import analysis
from synth.model.analysis import Round, Call, Country, Discipline, SpecificDiscipline
from synth.model.rco_synthsys_live import t_NHM_Call, NHMDiscipline, NHMSpecificDiscipline, \
    CountryIsoCode
from synth.utils import Step, Context, SynthRound


def get_steps(config, with_data=True):
    """
    Returns the ETL steps. If the with_data flag is passed as False then the data transform are
    omitted and the tables are simply dropped and recreated.

    :param config: the Config object
    :param with_data: whether to transfer the data over too (default: True)
    :return: a list of ordered steps to perform the requested ETL
    """
    context = Context(config)
    steps = [
        ClearAnalysisDB(context),
        CreateAnalysisDB(context),
    ]
    if with_data:
        steps.extend([
            FillRoundTable(context),
            FillCallTable(context),
            FillCountryTable(context),
            FillDisciplineTable(context),
            FillSpecificDisciplineTable(context),
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
        if database_exists(self.context.config.target):
            engine = create_engine(self.context.config.target)
            analysis.Base.metadata.drop_all(engine)


class CreateAnalysisDB(Step):
    """
    This step creates all tables for the analysis database, if the database doesn't already exist.
    """

    @property
    def message(self):
        return 'Create new target database using model (if necessary)'

    def _run(self, *args, **kwargs):
        if not database_exists(self.context.config.target):
            create_database(self.context.config.target)
        engine = create_engine(self.context.config.target)
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
            # we order by NHM_Call.call but could easily use NHM_Call.dateOpen as they produce the
            # same order
            for call in source.query(t_NHM_Call).order_by(t_NHM_Call.c.call.asc()):
                # TODO: do we want to use the call.callID or start from 1?
                call_id = (offset * synth_round) + call.callID
                target.add(Call(id=call_id, round=synth_round, start=call.dateOpen,
                                end=call.dateClosed))


class FillCountryTable(Step):

    @property
    def message(self):
        return 'Fill Country table and mapping with data from ISO 3166-1 alpha-2'

    def _run(self, target, *args):
        """
        Fill the Country table with data from pycountry which is a library that wraps the most
        recent ISO databases for various country and language packages. Also, populate a translator
        with a map from the country codes to the new Country ids.
        """
        # add the new Country objects to the target session
        for country_id, country in enumerate(pycountry.countries, start=1):
            self.context.mapping_set(CountryIsoCode, country.alpha_2, country_id)
            target.add(Country(id=country_id, code=country.alpha_2, name=country.name))


class FillDisciplineTable(Step):

    @property
    def message(self):
        return 'Fill Discipline table with data'

    def _run(self, target, synth4, *args):
        """
        Fill the Discipline table with data from the NHM_Discipline table.
        Notes:
            - All databases have the same data in this table so we can just copy the synth 4 data
              and ignore the synth 1-3 NHM_Discipline data.
        """
        # add the new Discipline objects to the target session
        for discipline in synth4.query(NHMDiscipline).order_by(NHMDiscipline.DisciplineID.asc()):
            target.add(Discipline(id=discipline.DisciplineID, name=discipline.DisciplineName))


class FillSpecificDisciplineTable(Step):

    def __init__(self, context):
        super().__init__(context)
        # this will keep track of the SpecificDiscipline objects we've added (keyed on their names)
        self.added = {}
        # an id generator so that we can reference the ids in the code before committing the changes
        self.id_generator = itertools.count(1)

    @property
    def message(self):
        return 'Fill SpecificDiscipline table with data'

    def match_existing_specific_discipline(self, candidate):
        """
        Matches the given candidate specific discipline name to the given

        :param candidate: the candidate NHMSpecificDiscipline object
        :return: the SpecificDiscipline object that this candidate is a duplicate of or None
        """
        # TODO: fuzzy match the names/curate a mapping
        return self.added.get(candidate.SpecificDisciplineName, None)

    def _run(self, target, *synth_sources):
        """
        Fill the SpecificDiscipline table with data from the NHM_Specific_Discipline table.
        Notes:
            - We try to deduplicate the specific disciplines across the 4 synth databases by
              matching their names up using, currently, straight up equality
            - A translator is populated for the NHMSpecificDiscipline key during this step for use
              by later steps
        """
        for synth_round, source in zip(reversed(SynthRound), reversed(synth_sources)):
            for orig in source.query(NHMSpecificDiscipline).order_by(
                    NHMSpecificDiscipline.SpecificDisciplineID.asc()):
                existing = self.match_existing_specific_discipline(orig)

                if existing:
                    if orig.DisciplineID == existing.discipline_id:
                        self.context.mapping_set(NHMSpecificDiscipline, orig.SpecificDisciplineID,
                                                 existing.id, synth=synth_round)
                    else:
                        raise SpecificDisciplineParentMismatch(
                            synth_round, orig.SpecificDisciplineID, orig.DisciplineID,
                            existing.discipline_id)
                else:
                    new_id = next(self.id_generator)
                    new = SpecificDiscipline(id=new_id, name=orig.SpecificDisciplineName,
                                             discipline_id=orig.DisciplineID)

                    self.added[orig.SpecificDisciplineName] = new
                    self.context.mapping_set(NHMSpecificDiscipline, orig.SpecificDisciplineID,
                                             new_id, synth=synth_round)
                    target.add(new)
