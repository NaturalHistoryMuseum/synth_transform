import abc
import csv
import enum
import json
from pathlib import Path

import requests
from crossref.restful import Works

from synth.errors import DuplicateUserGUIDError
from synth.model.rco_synthsys_live import NHMOutput
from synth.utils import Step, find_doi


@enum.unique
class Resource(enum.Enum):
    INSTITUTIONS = 'institutions'
    DOIS = 'dois'
    USERS = 'users'


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


class OutputDOIs(JSONDataResource):
    """
    This resource is a cached set of DOI metadata pulled from Crossref's API for the DOIs present in
    the source synth databases. Having this cached speeds up the ETL processing and reduces our
    calls to the Crossref API which is the nice thing to do.

    Note that updating the cache can take about ~20 mins or so.
    """

    def __init__(self, context):
        super().__init__(context, DataResource.data_dir / 'output_dois.json')
        self.works = Works()

    def update(self, context, target, *synth_sources):
        self.data = {}
        for synth_db in synth_sources:
            for output in synth_db.query(NHMOutput).filter(NHMOutput.URL.ilike('%doi%')):
                doi = find_doi(output.URL)
                if doi:
                    doi_metadata = self.works.doi(doi)
                    if doi_metadata:
                        self.data[doi_metadata['DOI']] = doi_metadata

        # write the data dict
        super().update()


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
        SYNTH_1_ID = 'synth1_ID'
        SYNTH_2_ID = "synth2_ID"
        SYNTH_3_ID = "synth3_ID"
        SYNTH_4_ID = 'synth4_ID'
        SYNTH_1_AGE = "Synth round 1 age"
        SYNTH_2_AGE = "Synth round 2 age"
        SYNTH_3_AGE = "Synth round 3 age"
        SYNTH_4_AGE = "Synth round 4 age"

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
                guid = row.pop(Users.Columns.GUID.value)
                if guid in self.data:
                    raise DuplicateUserGUIDError(guid)
                else:
                    self.data[guid] = row

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
            if row[column.value] and int(row[column.value]) == user_id:
                return guid

    def lookup_age(self, synth_round, user_guid):
        """
        Given a synth round and a user's guid, lookup and return the age of the user in that round.

        :param synth_round: the SynthRound
        :param user_guid: the user's guid
        :return: the user's age during the round
        """
        return self.data[user_guid][Users.Columns.age_column(synth_round).value]


class RegisterResourcesStep(Step):
    """
    This step registers and loads all the resources we know about into the context.
    """

    @property
    def message(self):
        return 'Registering resource data files'

    def run(self, context, *args, **kwargs):
        resources = {
            Resource.INSTITUTIONS: Institutions(context),
            Resource.DOIS: OutputDOIs(context),
            Resource.USERS: Users(context),
        }
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
