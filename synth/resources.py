import abc
import json
from pathlib import Path

import requests

from synth.utils import Step


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
        self._data = None

    @property
    def data(self):
        """
        Retrieves the data held in this resource, loading it if necessary.
        :return: the data
        """
        if self._data is None:
            self.load()
        return self._data

    @abc.abstractmethod
    def load(self):
        """
        Loads the resource's data.
        """
        pass

    @abc.abstractmethod
    def update(self):
        """
        Updates the resource's data on disk and in memory.
        """
        pass


class Institutions(DataResource):
    """
    Cleaned up aliases for institution names from the Vizzuality synth 3 GitHub repo.
    """

    def __init__(self, context):
        super().__init__(context)
        self.path = DataResource.data_dir / 'master_clean.json'

    def load(self):
        with open(self.path, 'r') as f:
            self._data = json.load(f)

    def update(self):
        url = 'https://raw.githubusercontent.com/Vizzuality/Synthesys3/master/Data/' \
              'master_clean.json'

        r = requests.get(url)
        r.raise_for_status()

        # update the internal version of the data we have
        self._data = r.json()

        # dump it nicely
        with open(self.path, 'w') as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)


class RegisterResourcesStep(Step):
    """
    This step registers and loads all the resources we know about into the context.
    """

    @property
    def message(self):
        return 'Registering resource data files'

    def run(self, context, *args, **kwargs):
        resources = {
            'institutions': Institutions(context),
        }
        for resource in resources.values():
            resource.load()
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
        self.resource.update()


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
