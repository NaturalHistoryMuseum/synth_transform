import abc
import csv
import enum
import json
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click
import pandas as pd
import requests
from crossref.restful import Works, Etiquette
from fuzzywuzzy import fuzz
from pandas import ExcelWriter
from sqlalchemy import or_
from sqlitedict import SqliteDict
from tqdm.contrib.concurrent import thread_map

from synth.errors import DuplicateUserGUIDError
from synth.model.rco_synthsys_live import NHMOutput
from synth.parsers.doi import DOIExtractor
from synth.utils import Step, SynthRound, find_names, clean_string


@enum.unique
class Resource(enum.Enum):
    INSTITUTIONS = 'institutions'
    DOIS = 'dois'
    DOIMETADATA = 'doimetadata'
    USERS = 'users'
    ACCESSREQUESTREBUILD = 'accessrequestrebuild'
    UNMATCHEDHOMEINSTITUTIONS = 'unmatchedhomeinstitutions'


class DataResource(abc.ABC):
    """
    Class representing a data resource. This will probably be a file in the synth/data directory.
    """
    # path to the synth/data directory
    data_dir = Path(__file__).parent / 'data'

    def __init__(self, context):
        """
        :param context: the context this resource is attached to
        """
        self.context = context

    @abc.abstractmethod
    def load(self, context, target, synth1, synth2, synth3, synth4):
        """
        Loads the resource's data.
        """
        pass

    @abc.abstractmethod
    def update(self, context, target, synth1, synth2, synth3, synth4):
        """
        Updates the resource's data on disk and in memory.
        """
        pass


class JSONDataResource(DataResource, abc.ABC):
    """
    Class representing a data resource in JSON format.
    """

    def __init__(self, context, path):
        super().__init__(context)
        self.path = path
        self.data = {}

    def load(self, *args, **kwargs):
        if self.path.exists():
            with open(self.path, 'r') as f:
                self.data = json.load(f)

    def update(self, *args, **kwargs):
        # dump the data nicely
        with open(self.path, 'w') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def get(self, key, default=None):
        return self.data.get(key, default)


class SqliteDataResource(DataResource, abc.ABC):
    """
    Class representing a data resource in SQLiteDict format (.db file, key-value store).
    """

    def __init__(self, context, path):
        super().__init__(context)
        self.path = path
        self.data = None

    @property
    def keys(self):
        return list(self.data.keys())

    def load(self, *args, **kwargs):
        """
        Use 'with x:' syntax instead to prevent resource being left open.
        """
        pass

    def update(self, *args, **kwargs):
        """
        Clear the resource, ready for new data.
        """
        if self.data is None:
            raise Exception('Resource is not open.')
        self.data.clear()

    def get(self, key, default=None):
        if self.data is None:
            raise Exception('Resource is not open.')
        return self.data.get(key, default)

    def add(self, key, value):
        if self.data is None:
            raise Exception('Resource is not open.')
        self.data[key] = value

    def __enter__(self):
        self.data = SqliteDict(str(self.path))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.data.commit()
        self.data.close()
        self.data = None


class XLSXDataResource(DataResource):

    def __init__(self, context, path):
        super().__init__(context)
        self.path = path
        self.data = {}

    def load(self, context, target, synth1, synth2, synth3, synth4):
        if self.path.exists():
            self.data = pd.read_excel(self.path, sheet_name=None)

    def update(self, context, target, synth1, synth2, synth3, synth4):
        with ExcelWriter(self.path) as writer:
            for sheet_name, data in self.data:
                data.to_excel(writer, sheet_name=sheet_name)


class Institutions(JSONDataResource):
    """
    Cleaned up aliases for institution names from the Vizzuality synth 3 GitHub repo.
    """

    def __init__(self, context):
        super().__init__(context, DataResource.data_dir / 'master_clean.json')

    def update(self, *args, **kwargs):
        r = requests.get('https://raw.githubusercontent.com/Vizzuality/Synthesys3/master/Data/'
                         'master_clean.json')
        r.raise_for_status()
        self.data = r.json()
        # write the data dict
        super().update()


class DOIMetadata(SqliteDataResource):
    """
    This resource is a cached set of DOI metadata pulled from Crossref's API for the DOIs saved in the OutputDOIs
    resource. Having this cached speeds up the ETL processing and reduces our calls to the Crossref API which is the
    nice thing to do.
    """

    def __init__(self, context):
        super().__init__(context, DataResource.data_dir / 'doi_metadata.db')
        etiquette = Etiquette('SYNTH transform', '0.1', 'https://github.com/NaturalHistoryMuseum/synth_transform',
                              'data@nhm.ac.uk')
        self.works = Works(etiquette=etiquette)
        self._handled = set()  # all the dois that are checked in this run
        self._added = set()  # all the dois that are added in this run
        self._errors = {}

    def _get_metadata(self, conn, doi):
        """
        Retrieve metadata for a single DOI and add it to a SqliteDataResource with an open SQLiteDict.
        :param conn: SqliteDataResource with an open SQLiteDict, e.g. 'self' within 'with self:'
        :param doi: the DOI to search crossref for
        """
        if doi is None:
            return
        self._handled.add(doi)
        try:
            doi_metadata = self.works.doi(doi)
            if doi_metadata:
                conn.add(doi_metadata['DOI'].upper(), doi_metadata)
                self._added.add(doi)
        except Exception as e:
            self._errors[doi] = e

    def update(self, context, target, *synth_sources):
        """
        Retrieve and store metadata for each the DOIs stored in the OutputDOIs resource.
        """
        with self:
            super(DOIMetadata, self).update(context, target, *synth_sources)
        self._handled = set()
        self._added = set()
        self._errors = {}

        doi_cache = OutputDOIs(context)
        with doi_cache:
            found_dois = list(set(doi_cache.data.values()))

        workers = context.config.resource_opt('doimetadata.threads', 20)
        with self, ThreadPoolExecutor(workers) as executor:
            thread_map(lambda x: self._get_metadata(self, x), found_dois, desc='Crossref', unit=' dois', leave=False,
                       position=1)


class OutputDOIs(SqliteDataResource):
    """
    This resource is a cached set of output IDs matched to DOIs using regexes, URLs, Crossref searches, and Refindit
    searches. This resource takes several hours to update, depending on throttling from Crossref and number of threads
    available for multiprocessing.
    """

    def __init__(self, context):
        super().__init__(context, DataResource.data_dir / 'output_dois.db')
        etiquette = Etiquette('SYNTH transform', '0.1', 'https://github.com/NaturalHistoryMuseum/synth_transform',
                              'data@nhm.ac.uk')
        self.works = Works(etiquette=etiquette)
        self._handled = set()
        self._added = set()
        self._errors = {}
        self._methods = {}

    @property
    def keys(self):
        return [tuple(json.loads(k)) for k in self.data.keys()]

    def mapped_items(self, new_id_map):
        """
        Transform the stored keys (tuples of (synth round, output ID)) into new IDs using a map generated during the
        rebuild process. Resource must be open.
        :param new_id_map: a dict with tuple keys and new ID values
        """
        if self.data is None:
            raise Exception('Resource is not open.')
        mapped = {}
        for k, v in self.data.items():
            try:
                new_key = new_id_map[tuple(json.loads(k))]
                mapped[new_key] = v
            except KeyError:
                continue
        return mapped

    def _search_output(self, conn, output, synth_round):
        """
        Search for a single output using title and author. Searches the Crossref API first, then ReFindIt if that
        doesn't return a suitable result. Compares the output title with each result using fuzzywuzzy and considers
        them a match if the two strings are at least 80% similar.
        :param conn: SqliteDataResource with an open SQLiteDict, e.g. 'self' within 'with self:'
        :param output: the Output instance we're attempting to find a DOI for
        :param synth_round: the round this output was recorded in
        """
        output_key = json.dumps((synth_round, output.Output_ID))
        self._handled.add(output_key)
        try:
            authors = find_names(clean_string(output.Authors) or '')
            title = output.Title.rstrip('.')
            q = self.works.query(author=authors, bibliographic=title).sort('relevance').order('desc')
            for ri, result in enumerate(q):
                result_title = result.get('title', [None])[0]
                if result_title is None:
                    continue
                similarity = fuzz.partial_ratio(result_title, title.lower())
                if similarity >= 80:
                    self._added.add(output.Output_ID)
                    conn.add(output_key, result['DOI'].upper())
                    self._methods[output_key] = 'crossref'
                    return
                if ri >= 3 - 1:
                    return
            # refindit also searches a few other databases, so try that if crossref doesn't find it
            refindit_url = 'https://refinder.org/find?search=advanced&limit=5&title=' \
                           f'{title}&author={"&author=".join(authors)}'
            refindit_response = requests.get(refindit_url)
            if refindit_response.ok:
                for ri, result in enumerate(refindit_response.json()):
                    result_title = result.get('title')
                    if result_title is None:
                        continue
                    similarity = fuzz.partial_ratio(result_title, title.lower())
                    if similarity >= 80:
                        self._added.add(output.Output_ID)
                        conn.add(output_key, result['DOI'].upper())
                        self._methods[output_key] = 'refindit'
                        return
        except Exception as e:
            self._errors[(synth_round, output.Output_ID)] = e

    def update(self, context, target, *synth_sources):
        """
        Attempt to find a DOI for each output in the NHMOutput tables.
        """
        with self:
            super(OutputDOIs, self).update(context, target, *synth_sources)
        self._handled = set()
        self._errors = {}
        self._methods = {}

        for db_ix, synth_db in enumerate(synth_sources):
            db_ix += 1
            self._added = set()

            def _extract_doi(conn, output, col):
                output_key = json.dumps((db_ix, output.Output_ID))
                self._handled.add(output_key)
                for x in DOIExtractor.dois(getattr(output, col), fix=True):
                    doi, fn = x
                    doi_metadata = self.works.doi(doi)
                    if doi_metadata:
                        doi_title = doi_metadata.get('title', '')
                        doi_title = clean_string(doi_title[0]).lower()
                        output_title = output.Title
                        if output_title is not None:
                            output_title = clean_string(output_title.lower())
                        match = fuzz.partial_ratio(doi_title, output_title)
                        if match > 50:
                            self._added.add(output.Output_ID)
                            conn.add(output_key, doi.upper())
                            self._methods[output_key] = fn
                            break

            def _search_columns(col, *filters):
                outputs = synth_db.query(NHMOutput).filter(NHMOutput.Output_ID.notin_(self._added), *filters)
                thread_workers = context.config.resource_opt('dois.threads', 20)
                with self, ThreadPoolExecutor(thread_workers) as thread_executor:
                    thread_map(lambda x: _extract_doi(self, x, col), outputs.all(), desc=col, unit=' records',
                               leave=False, position=1)

            _search_columns('URL', NHMOutput.URL.isnot(None))
            _search_columns('Volume', or_(NHMOutput.Volume.ilike('%doi%'), NHMOutput.Volume.ilike('%10.%/%')))
            _search_columns('Pages', or_(NHMOutput.Pages.ilike('%doi%'), NHMOutput.Pages.ilike('%10.%/%')))

            # now for searching based on metadata
            title_and_author = synth_db.query(NHMOutput).filter(NHMOutput.Output_ID.notin_(self._added),
                                                                NHMOutput.Title.isnot(None),
                                                                NHMOutput.Authors.isnot(None))

            workers = context.config.resource_opt('dois.threads', 20)
            with self, ThreadPoolExecutor(workers) as executor:
                thread_map(lambda x: self._search_output(self, x, db_ix), title_and_author.all(), desc='Crossref',
                           unit=' records', leave=False, position=1)

        methods = {}
        for k, v in self._methods.items():
            methods[v] = methods.get(v, []) + [k]

        for k, v in methods.items():
            click.echo(f'{k}: {len(v)}')


class Users(DataResource):
    """
    This resource represents a map between the unique identifiers in each of the synth databased for
    a given user and a new unique identifer (the GUID) for that user. This allows us to track the
    users across the databases. This data is compiled offline because of risks with PII. This is
    also why it includes age data in the form of the user's age range at the time of each of the
    synth rounds.
    """

    @enum.unique
    class Columns(enum.Enum):
        # cheeky enum to define the columns in the csv
        GUID = 'GUID'
        SYNTH_1_ID = 'synth1'
        SYNTH_2_ID = "synth2"
        SYNTH_3_ID = "synth3"
        SYNTH_4_ID = 'synth4'
        SYNTH_1_AGE = "synth round 1 age"
        SYNTH_2_AGE = "synth round 2 age"
        SYNTH_3_AGE = "synth round 3 age"
        SYNTH_4_AGE = "synth round 4 age"

        @staticmethod
        def id_column(synth_round):
            """
            Given a synth round, returns the appropriate Columns enum for the user's ID in that
            round.

            :param synth_round: the synth round
            :return: a Columns enum
            """
            return Users.Columns[f'SYNTH_{synth_round.value}_ID']

        @staticmethod
        def age_column(synth_round):
            """
            Given a synth round, returns the appropriate Columns enum for the user's age in that
            round.

            :param synth_round: the synth round
            :return: a Columns enum
            """
            return Users.Columns[f'SYNTH_{synth_round.value}_AGE']

    def __init__(self, context):
        super().__init__(context)
        self.path = DataResource.data_dir / 'users.csv'
        self.data = {}

    def load(self, *args, **kwargs):
        with open(self.path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                guid = row[Users.Columns.GUID.value]
                if guid in self.data:
                    raise DuplicateUserGUIDError(guid)
                else:
                    user = {}
                    for synth_round in SynthRound:
                        # first deal with the id from the synth round
                        id_column = Users.Columns.id_column(synth_round)
                        id_value = row[id_column.value]
                        if id_value:
                            user[id_column] = set(map(int, row[id_column.value].split(',')))
                        else:
                            user[id_column] = set()

                        # then deal with the age for the synth round
                        age_column = Users.Columns.age_column(synth_round)
                        age_value = row[age_column.value]
                        user[age_column] = age_value if age_value else None

                    self.data[guid] = user

    def update(self, *args, **kwargs):
        # sadly creating the csv is an offline process to avoid exposing PII in both this code and
        # the databases, therefore it can't be updated here
        pass

    def lookup_guid(self, synth_round, user_id):
        """
        Given a synth round and a user id in that round, lookup and return the user's assigned GUID.

        :param synth_round: the SynthRound
        :param user_id: the user's ID in the synth round
        :return: the guid or None if no guid was identified
        """
        user_id = int(user_id)
        column = Users.Columns.id_column(synth_round)
        for guid, row in self.data.items():
            if user_id in row[column]:
                return guid

    def lookup_age(self, synth_round, user_guid):
        """
        Given a synth round and a user's guid, lookup and return the age of the user in that round.

        :param synth_round: the SynthRound
        :param user_guid: the user's guid
        :return: the user's age during the round
        """
        return self.data[user_guid][Users.Columns.age_column(synth_round)]


class AccessRequestRebuild(XLSXDataResource):

    def __init__(self, context):
        super().__init__(context, DataResource.data_dir / 'access_request_rebuild.xlsx')

    @property
    def access_requests(self):
        return self.data['AccessRequest']

    @property
    def installation_facility(self):
        return self.data['InstallationFacility']

    @property
    def category(self):
        return self.data['Category']

    @property
    def institution(self):
        return self.data['Institution']


class UnmatchedHomeInstitutions(JSONDataResource):

    def __init__(self, context):
        super().__init__(context, self.data_dir / 'unmatched_home_institutions.json')


class RegisterResourcesStep(Step):
    """
    This step registers and loads all the resources we know about into the context.
    """

    @property
    def message(self):
        return 'Registering resource data files'

    def run(self, context, *args, **kwargs):
        # use an OrderedDict because OutputDOIs should always be run before DOIMetadata
        resources = OrderedDict((
            (Resource.INSTITUTIONS, Institutions(context)),
            (Resource.DOIS, OutputDOIs(context)),
            (Resource.DOIMETADATA, DOIMetadata(context)),
            (Resource.USERS, Users(context)),
            (Resource.ACCESSREQUESTREBUILD, AccessRequestRebuild(context)),
            (Resource.UNMATCHEDHOMEINSTITUTIONS, UnmatchedHomeInstitutions(context)),
        ))
        for resource in resources.values():
            resource.load(context, *args, **kwargs)
        context.resources.update(resources)


class UpdateResourceStep(Step):
    """
    This step updates a single resource.
    """

    def __init__(self, name, resource):
        self.name = name
        self.resource = resource

    @property
    def message(self):
        return f'Updating resource {self.name.value}'

    def run(self, *args, **kwargs):
        self.resource.update(*args, **kwargs)


def update_resources_tasks(context, *names):
    """
    Creates UpdateResourceSteps for each resource in the given context and returns them. If any
    names are provided then only those that are named are updated rather than all of them.

    :param context: the context object
    :param names: a series of names to update or no names to update all resources
    :return: a list of UpdateResourceStep objects
    """
    to_update = []
    if names:
        to_update.extend(name for name in context.resources.keys() if name in names)
    else:
        to_update.extend(context.resources.keys())

    return [UpdateResourceStep(name, context.resources[name]) for name in to_update]
