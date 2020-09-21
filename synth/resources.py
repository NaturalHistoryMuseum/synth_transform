import abc
import json
import enum
from pathlib import Path

import requests
from crossref.restful import Works

from synth.model.rco_synthsys_live import NHMOutput
from synth.utils import Step, find_doi


@enum.unique
class Resource(enum.Enum):
    INSTITUTIONS = 'institutions'
    DOIS = 'dois'


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
        return f'Updating resource {self.name}'

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
