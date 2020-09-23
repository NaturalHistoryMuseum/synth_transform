import itertools
import subprocess

import pycountry
from sqlalchemy import create_engine, func
from sqlalchemy_utils import create_database, database_exists

from synth.errors import SpecificDisciplineParentMismatch
from synth.model import analysis
from synth.model.analysis import Round, Call, Country, Discipline, SpecificDiscipline, Output
from synth.model.rco_synthsys_live import t_NHM_Call, NHMDiscipline, NHMSpecificDiscipline, \
    CountryIsoCode, NHMOutputType, NHMPublicationStatu, NHMOutput
from synth.resources import Resource
from synth.utils import Step, SynthRound, find_doi


# TODO: we should put all ids in the context mapping tables to avoid having to check if all the
#       synth sources are the same, then everything follows the same pattern even if it doesn't
#       technically need the mapping (example, NHMDiscipline)


def etl_steps(with_data=True):
    """
    Returns the ETL steps. If the with_data flag is passed as False then the data transform are
    omitted and the tables are simply dropped and recreated.

    :param with_data: whether to transfer the data over too (default: True)
    :return: a list of ordered steps to perform the requested ETL
    """
    steps = [
        ClearAnalysisDB(),
        CreateAnalysisDB(),
    ]
    if with_data:
        steps.extend([step() for step in (
            FillRoundTable,
            FillCallTable,
            FillCountryTable,
            FillDisciplineTable,
            FillSpecificDisciplineTable,
            FillOutputTable,
            CleanOutputsTable,
        )])

    return steps


class GenerateSynthDatabaseModel(Step):
    """
    This step uses sqlacodegen to automatically create an SQLAlchemy model for the source synth
    databases. They all use the same model so we read the synth 4 schema.
    """

    def __init__(self, filename):
        super().__init__()
        self.filename = filename

    @property
    def message(self):
        return 'Generating the model for source synth databases'

    def run(self, context, *args, **kwargs):
        with open(self.filename, 'w') as f:
            subprocess.call(['sqlacodegen', f'{context.config.sources[-1]}'], stdout=f)


class ClearAnalysisDB(Step):
    """
    This step drops all tables from the analysis database, if there is one.
    """

    @property
    def message(self):
        return 'Drop tables in target database (if necessary)'

    def run(self, context, *args, **kwargs):
        if database_exists(context.config.target):
            engine = create_engine(context.config.target)
            analysis.Base.metadata.drop_all(engine)


class CreateAnalysisDB(Step):
    """
    This step creates all tables for the analysis database, if the database doesn't already exist.
    """

    @property
    def message(self):
        return 'Create new target database using model (if necessary)'

    def run(self, context, *args, **kwargs):
        if not database_exists(context.config.target):
            create_database(context.config.target)
        engine = create_engine(context.config.target)
        analysis.Base.metadata.create_all(engine)


class FillRoundTable(Step):
    """
    Fills the Round table with the synth round data.
    """

    @property
    def message(self):
        return 'Fill round table with data'

    def run(self, context, target, *synth_sources):
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

    def run(self, context, target, *synth_sources):
        """
        Fill the Call table with data from the NHM_Call tables in each of the synth sources.
        """
        id_generator = itertools.count(1)
        for synth_round, source in zip(SynthRound, synth_sources):
            # we order by NHM_Call.call but could easily use NHM_Call.dateOpen as they produce the
            # same order
            for call in source.query(t_NHM_Call).order_by(t_NHM_Call.c.call.asc()):
                call_id = next(id_generator)
                context.mapping_set(t_NHM_Call, call.callID, call_id, synth=synth_round)
                target.add(Call(id=call_id, round_id=synth_round.value, start=call.dateOpen,
                                end=call.dateClosed))


class FillCountryTable(Step):

    @property
    def message(self):
        return 'Fill Country table and mapping with data from ISO 3166-1 alpha-2'

    def run(self, context, target, *args, **kwargs):
        """
        Fill the Country table with data from pycountry which is a library that wraps the most
        recent ISO databases for various country and language packages. Also, populate a translator
        with a map from the country codes to the new Country ids.
        """
        # add the new Country objects to the target session
        for country_id, country in enumerate(pycountry.countries, start=1):
            context.mapping_set(CountryIsoCode, country.alpha_2, country_id)
            target.add(Country(id=country_id, code=country.alpha_2, name=country.name))


class FillDisciplineTable(Step):

    @property
    def message(self):
        return 'Fill Discipline table with data'

    def run(self, context, target, synth4, *args, **kwargs):
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

    def __init__(self):
        super().__init__()
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

    def run(self, context, target, *synth_sources):
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
                        context.mapping_set(NHMSpecificDiscipline, orig.SpecificDisciplineID,
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
                    context.mapping_set(NHMSpecificDiscipline, orig.SpecificDisciplineID,
                                        new_id, synth=synth_round)
                    target.add(new)


class FillOutputTable(Step):

    @property
    def message(self):
        return 'Fill Outputs table with data'

    def run(self, context, target, synth1, synth2, synth3, synth4):
        """
        Fill the Output table with data from the NHM_Output table.
        Notes:
            - we denormalise the output type and publication status links
            - years are converted to ints if they're not null
        """
        # extract the output types and publication statuses from the synth 4 database (all databases
        # have the same data in these tables)
        output_types = {
            output_type.OutputType_ID: output_type.OutputType
            for output_type in synth4.query(NHMOutputType)
        }
        publication_statuses = {
            output_type.PublicationStatus_ID: output_type.PublicationStatus
            for output_type in synth4.query(NHMPublicationStatu)
        }

        id_generator = itertools.count(1)
        for synth_round, source in zip(SynthRound, (synth1, synth2, synth3, synth4)):
            for output in source.query(NHMOutput):
                new_id = next(id_generator)

                # add a mapping from the old id to the new id
                context.mapping_set(NHMOutput, output.Output_ID, new_id, synth=synth_round)

                # add the Output to the target db
                target.add(Output(
                    id=new_id,
                    # TODO: probably needs to be mapped
                    # userID=
                    output_type=output_types.get(output.OutputType_ID, None),
                    publication_status=publication_statuses.get(output.PublicationStatus_ID, None),
                    authors=output.Authors,
                    year=int(output.Year) if output.Year is not None else None,
                    title=output.Title,
                    publisher=output.Publisher,
                    url=output.URL,
                    volume=output.Volume,
                    pages=output.Pages,
                    conference=output.Conference,
                    degree=output.Degree
                ))


class CleanOutputsTable(Step):

    def __init__(self):
        super().__init__()

    @property
    def message(self):
        return 'Clean the outputs table up'

    @staticmethod
    def update_output_from_doi(output, doi):
        """
        Given an Output model object and a dict of DOI metadata, update the Output object with the
        metadata from the DOI metadata dict.

        :param output: an Output model object
        :param doi: a dict of DOI metadata from Crossref's API
        """
        authors = []
        for author in doi['author']:
            if 'given' in author and 'family' in author:
                authors.append(f"{author['family']} {author['given']}")

        output.authors = '; '.join(authors)
        output.year = int(doi['created']['date-time'][:4])
        output.title = doi['title'][0]
        output.publisher = doi['publisher']
        output.url = doi['URL']
        if 'volume' in doi:
            output.volume = doi['volume']
        if 'page' in doi:
            output.pages = doi['page']

    def run(self, context, target, *args, **kwargs):
        """
        Clean up the outputs table in the target database by attempting to match the entries in the
        table to papers in Crossref's API.
        """
        handled = set()
        # look for DOIs first cause they should be able to provide us with nice clean metadata
        for output in target.query(Output).filter(Output.url.ilike('%doi%')):
            doi = find_doi(output.url)
            doi_metadata = context.resources[Resource.DOIS].get(doi, None)
            if doi_metadata:
                self.update_output_from_doi(output, doi_metadata)
                handled.add(output.id)

        # TODO: add more ways of matching outputs in the Crossref API
