import itertools
import subprocess
from datetime import datetime
from numbers import Number

import pycountry
from sqlalchemy import create_engine, func
from sqlalchemy.schema import CreateTable
from sqlalchemy_utils import create_database, database_exists

from synth.errors import SpecificDisciplineParentMismatch
from synth.model import analysis
from synth.model.analysis import Round, Call, Country, Discipline, SpecificDiscipline, Output, \
    VisitorProject, Category, Institution, InstallationFacility, AccessRequest
from synth.model.rco_synthsys_live import t_NHM_Call, NHMDiscipline, NHMSpecificDiscipline, \
    CountryIsoCode, NHMOutputType, NHMPublicationStatu, NHMOutput, TListOfUserProject, TListOfUser
from synth.resources import Resource, RegisterResourcesStep
from synth.utils import Step, SynthRound, clean_string, to_datetime, clean_institution


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
        # add all the other steps
        steps.extend([step() for step in (
            RegisterResourcesStep,
            FillRoundTable,
            FillCallTable,
            FillCountryTable,
            FillDisciplineTable,
            FillSpecificDisciplineTable,
            FillOutputTable,
            CleanOutputsTable,
            FillVisitorProjectTable,
            FillCategoryTable,
            FillInstitutionTable,
            FillInstallationFacilityTable,
            FillAccessRequestTable,
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


class DumpAnalysisDatabase(Step):
    """
    This step dumps the entire analysis database into a single SQL file containing table creation
    DDL and then insert statements to create the rows. The resulting file can then be run on any
    other MySQL server to reproduce the analysis database.
    """

    def __init__(self, path):
        """
        :param path: the Path where the dump should be written
        """
        super().__init__()
        self.path = path

    @property
    def message(self):
        return 'Generates a dump of the analysis database using mysqldump'

    @staticmethod
    def serialise(value):
        """
        Helper function which defines how we dump certain data types for the SQL insert statements.
        Currently this handles booleans, nulls, strings, datetimes and numbers.

        :param value: a database value
        :return: the value ready for inclusion in the insert statement
        """
        # booleans are dumped using the TRUE or FALSE MySQL keywords
        if isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'

        # Nones need to be nulls
        if value is None:
            return 'null'

        if isinstance(value, (str, datetime)):
            # strings and datetimes need to have single quotes escaped and then the whole string
            # must be wrapped in single quotes
            formatted = str(value).replace("'", r"\'")
            return f"'{formatted}'"

        # numbers are fine, just wrap as a str
        if isinstance(value, Number):
            return str(value)

        # we could do return str(value) here but to avoid subtle errors in the future it makes more
        # sense to error if the value is of a type we don't have an explicit mapping for, therefore
        # error
        raise Exception(f'No serialisation mapping exists for {value} of type {type(value)}')

    def run(self, context, target, *args, **kwargs):
        with open(self.path, 'w') as f:
            # first generate all the create DDL statements
            for table in analysis.Base.metadata.sorted_tables:
                f.write(f'# create for {table.name}\n')
                f.write(f'{str(CreateTable(table).compile(target.bind)).strip()}\n')
                f.write('\n')

            f.write('\n')

            # then dump the actual data
            for table in analysis.Base.metadata.sorted_tables:
                f.write(f'# data for {table.name}\n')
                for row in target.query(table):
                    values = ', '.join(map(self.serialise, row))
                    f.write(f'INSERT INTO {table.name} VALUES ({values});\n')
                f.write('\n')


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
        return 'Create new target database using model'

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
                context.map(t_NHM_Call, call.callID, call_id, synth_round=synth_round)
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
            context.map(CountryIsoCode, country.alpha_2, country_id)
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
            context.map(NHMDiscipline, discipline.DisciplineID, discipline.DisciplineID)
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

                mapped_discipline_id = context.translate(NHMDiscipline, orig.DisciplineID,
                                                         synth_round)

                if existing:
                    if mapped_discipline_id == existing.discipline_id:
                        # also map this specific discipline id to another existing id that we've
                        # already added
                        context.map(NHMSpecificDiscipline, orig.SpecificDisciplineID,
                                    existing.id, synth_round=synth_round)
                    else:
                        raise SpecificDisciplineParentMismatch(
                            synth_round, orig.SpecificDisciplineID, orig.DisciplineID,
                            existing.discipline_id)
                else:
                    new_id = next(self.id_generator)
                    new = SpecificDiscipline(id=new_id, name=orig.SpecificDisciplineName,
                                             discipline_id=mapped_discipline_id)

                    self.added[orig.SpecificDisciplineName] = new
                    context.map(NHMSpecificDiscipline, orig.SpecificDisciplineID,
                                new_id, synth_round=synth_round)
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
                context.map(NHMOutput, output.Output_ID, new_id, synth_round=synth_round)

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
    def update_output_from_doi_metadata(output, doi_metadata):
        """
        Given an Output model object and a dict of DOI metadata, update the Output object with the
        metadata from the DOI metadata dict.

        :param output: an Output model object
        :param doi_metadata: a dict of DOI metadata from Crossref's API
        """
        authors = []
        for author in doi_metadata.get('author', []):
            if 'given' in author and 'family' in author:
                authors.append(f"{author['family']} {author['given']}")

        titles = doi_metadata['title']
        if len(titles) == 0:
            title = output.title
        else:
            title = clean_string(titles[0])

        output.authors = '; '.join(authors)
        output.year = int(doi_metadata['created']['date-time'][:4])
        output.title = title
        output.publisher = doi_metadata['publisher']
        output.url = doi_metadata['URL']
        if 'volume' in doi_metadata:
            output.volume = doi_metadata['volume']
        if 'page' in doi_metadata:
            output.pages = doi_metadata['page']

    def run(self, context, target, *args, **kwargs):
        """
        Clean up the outputs table in the target database by attempting to match the entries in the
        table to papers in Crossref's API.
        """
        doi_cache = context.resources[Resource.DOIS]
        metadata_cache = context.resources[Resource.DOIMETADATA]
        handled = set()
        added = set()

        # attempt to clean up the authors to make it easier to search for them later
        for output in target.query(Output).filter(Output.authors.isnot(None),
                                                  Output.authors != ''):
            output.authors = clean_string(output.authors)

        # clean the titles as well
        for output in target.query(Output).filter(Output.title.isnot(None),
                                                  Output.title != ''):
            output.title = clean_string(output.title)

        # update from the cached matched DOIs and metadata (recommended that the update methods are
        # run before this)
        with doi_cache, metadata_cache:
            mapped_items = doi_cache.mapped_items(context.mappings[NHMOutput])
            for output in target.query(Output).filter(Output.id.in_(mapped_items.keys())):
                handled.add(output.id)
                doi = mapped_items.get(output.id)
                if doi is not None:
                    doi_metadata = metadata_cache.get(doi)
                    if doi_metadata:
                        self.update_output_from_doi_metadata(output, doi_metadata)
                        added.add(output.id)


class FillVisitorProjectTable(Step):

    @property
    def message(self):
        return 'Fill VisitorProject table with data'

    def run(self, context, target, *synth_sources):
        """
        Fill the monster VisitorProject table with data. This data is a mix of stuff from the source
        projects and users tables (TListOfUserProject and TListOfUser).

        :param context:
        :param target:
        :param synth_sources:
        :return:
        """
        users = context.resources[Resource.USERS]
        institution_lookup = context.resources[Resource.INSTITUTIONS].data
        id_generator = itertools.count(1)

        for synth_round, source in zip(SynthRound, synth_sources):
            # grab only the projects that have been "completed"
            projects = source.query(TListOfUserProject) \
                .filter(TListOfUserProject.Application_State != 'edit') \
                .order_by(TListOfUserProject.UserProject_ID.asc())

            # grab a list of the calls for this synth round, in order
            calls = target.query(Call) \
                .filter(Call.round_id == synth_round.value) \
                .order_by(Call.id.asc()) \
                .all()

            for project in projects:
                user_guid = users.lookup_guid(synth_round, project.User_ID)
                if user_guid is None:
                    # this is assumed to be fine and simply implies that the user's project or the
                    # user was eliminated by the process that creates the users.csv file. We can
                    # safely ignore this project and just move on
                    continue

                user = source.query(TListOfUser).get(project.User_ID)

                # work out which call the project was submitted against
                call_submitted = calls[int(project.Call_Submitted) - 1].id

                visitor_project_id = next(id_generator)

                context.map(TListOfUserProject, project.UserProject_ID, visitor_project_id,
                            synth_round)

                visitor_project = VisitorProject(
                    id=visitor_project_id,
                    ############ project based info ############
                    title=project.UserProject_Title,
                    objectives=project.UserProject_Objectives,
                    achievements=project.UserProject_Achievements,
                    user_guid=user_guid,
                    user_age_range=users.lookup_age(synth_round, user_guid),
                    length_of_visit=project.length_of_visit,
                    start=project.start_date,
                    end=project.finish_date,
                    taf_id=project.TAF_ID,
                    home_facilities=bool(project.Home_Facilities),
                    application_state=project.Application_State,
                    acceptance=project.Acceptance,
                    summary=project.UserProject_Summary,
                    new_user=bool(project.New_User),
                    facility_reasons=project.UserProject_Facility_Reasons,
                    submission_date=to_datetime(project.Submission_Date),
                    support_final=bool(project.Support_Final),
                    # note that all projects have the same ids for this table so this is fine
                    project_discipline=project.Project_Discipline,
                    project_specific_discipline=context.translate(
                        NHMSpecificDiscipline, project.Project_Specific_Discipline, synth_round),
                    call_submitted=call_submitted,
                    previous_application=bool(project.Previous_Application),
                    training_requirement=project.Training_Requirement,
                    # TODO: should use lookup and get id for?
                    supporter_institution=clean_institution(institution_lookup,
                                                            project.Supporter_Institution),
                    administration_state=project.Administration_State,
                    group_leader=bool(project.Group_leader),
                    group_members=project.Group_Members,
                    background=project.UserProject_Background,
                    reasons=project.UserProject_Reasons,
                    expectations=project.UserProject_Expectations,
                    outputs=project.UserProject_Outputs,
                    # TODO: should use lookup and get id for?
                    group_leader_institution=clean_institution(institution_lookup,
                                                               project.Group_Leader_Institution),
                    visit_funded_previously=project.Visit_Funded_Previously,

                    ############ user based info ############
                    gender=user.Gender,
                    nationality=context.translate(CountryIsoCode, user.Nationality_Country_code,
                                                  synth_round),
                    researcher_status=user.Researcher_status,
                    researcher_discipline1=user.Discipline1,
                    researcher_discipline2=user.Discipline2,
                    researcher_discipline3=user.Discipline3,
                    home_institution_type=user.Home_Institution_Type,
                    home_institution_dept=user.Home_Institution_Dept,
                    home_institution_name=clean_institution(institution_lookup,
                                                            user.Home_Institution_Name),
                    home_institution_town=user.Home_Institution_Town,
                    home_institution_country=context.translate(CountryIsoCode,
                                                               user.Home_Institution_Country_code,
                                                               synth_round),
                    home_institution_postcode=user.Home_Institution_Postcode,
                    number_of_visits=user.Number_of_visits,
                    duration_of_stays=user.Duration_of_stays,
                    nationality_other=user.Nationality_OtherText,
                    remote_user=user.Remote_user,
                    travel_and_subsistence_reimbursed=user.Travel_and_Subsistence_reimbursed,
                    job_title=user.jobTitle
                )

                target.add(visitor_project)


class FillCategoryTable(Step):

    @property
    def message(self):
        return 'Fill Category table with data'

    def run(self, context, target, *synth_sources):
        """
        Fill the category table with the data from the access_request_rebuild.xlsx resource data
        file.
        """
        # get the sheet as a dataframe
        data = context.resources[Resource.ACCESSREQUESTREBUILD].category
        # loop over the rows and load them in
        for row in data.itertuples():
            target.add(Category(id=row.Category_ID, name=row.CategoryName,
                                higherName=row.HigherCategoryName))


class FillInstitutionTable(Step):

    @property
    def message(self):
        return 'Fill Institution table with data'

    def run(self, context, target, *synth_sources):
        """
        Fill the institution table with the data from the access_request_rebuild.xlsx resource data
        file.
        """
        # get the sheet as a dataframe
        data = context.resources[Resource.ACCESSREQUESTREBUILD].institution
        # loop over the rows and load them in
        for row in data.itertuples():
            # lookup the country in the analysis db rather than using translate given that this code
            # doesn't come from the database directly
            country = target.query(Country).filter(Country.code == row.CountryCode).one()
            target.add(Institution(id=row.Institution_ID, acronym=row.InstitutionAcronym,
                                   name=row.InstitutionName, country_id=country.id))


class FillInstallationFacilityTable(Step):

    @property
    def message(self):
        return 'Fill InstallationFacility table with data'

    def run(self, context, target, *synth_sources):
        """
        Fill the InstallationFacility table with the data from the access_request_rebuild.xlsx
        resource data file.
        """
        # get the sheet as a dataframe
        data = context.resources[Resource.ACCESSREQUESTREBUILD].installation_facility
        # loop over the rows and load them in
        for row in data.itertuples():
            target.add(InstallationFacility(id=row.InstallationFacility_ID,
                                            code=row.InstallationCode,
                                            description=row.InstallationFacilityDescription,
                                            category_id=row.Category_ID,
                                            institution_id=row.Institution_ID))


class FillAccessRequestTable(Step):

    @property
    def message(self):
        return 'Fill AccessRequest table with data'

    def run(self, context, target, *synth_sources):
        """
        Fill the AccessRequest table with the data from the access_request_rebuild.xlsx
        resource data file.
        """
        # get the sheet as a dataframe
        data = context.resources[Resource.ACCESSREQUESTREBUILD].access_requests

        for row in data.itertuples():
            visitor_project_id = context.translate(TListOfUserProject, row.UserProject_ID,
                                                   row.SynthRound)
            target.add(AccessRequest(id=row.AccessRequest_ID,
                                     visitor_project_id=visitor_project_id,
                                     installation_facility_id=row.InstallationFacility_ID,
                                     days_requested=row.DaysRequested,
                                     request_detail=row.RequestDetail))
