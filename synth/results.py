import abc
import csv
from pathlib import Path

import pandas as pd

from synth.utils import Step

import seaborn as sns


class UpdateResultStep(Step):
    """
    This step updates the associated csv output file using the given sql file.
    """

    def __init__(self, sql_file):
        """
        :param sql_file: the sql file Path
        """
        self.sql_file = sql_file
        self.csv_file = sql_file.with_suffix('.csv')

    @property
    def message(self):
        return f'Updating {self.sql_file.stem} output'

    def run(self, context, target, *args, **kwargs):
        with self.sql_file.open('rt') as f:
            # grab the results from the database
            result = target.execute(f.read())
            headers = result.keys()
            rows = result.fetchall()

            # now open the csv file and write the data we've got in memory, this avoids overwriting
            # the csv file with a partial result in the event of a database error
            with self.csv_file.open('w') as g:
                # set the lineterminator for consistency across platforms
                writer = csv.writer(g, lineterminator='\n')
                writer.writerow(headers)
                writer.writerows(rows)


class CSVChartStep(Step, abc.ABC):
    """
    Abstract step for creating charts from CSV results files.
    """

    def __init__(self, results_path):
        self.results_path = results_path

    @property
    @abc.abstractmethod
    def csv_file(self):
        """
        The csv Path this chart uses.
        :return: a Path object
        """
        pass

    @property
    def message(self):
        return f'Updating {self.csv_file.stem} chart'

    def run(self, context, target, *args, **kwargs):
        data = pd.read_csv(self.csv_file)
        chart = self.create_chart(data)
        chart.savefig(self.results_path / f'{self.csv_file.stem}.png')

    @abc.abstractmethod
    def create_chart(self, data):
        """
        Abstract method that should return a chart (Grid) generated from the given data.

        :param data: a pandas dataframe of the CSV file
        :return: a Grid object
        """
        pass


class UpdateVisitsAgeRangeChartStep(CSVChartStep):
    """
    Creates a chart for the visits_age_range_count.csv results file.
    """

    @property
    def csv_file(self):
        return self.results_path / 'visits_age_range_count.csv'

    def create_chart(self, data):
        sns.set_theme(style='whitegrid')
        g = sns.catplot(data=data, kind='bar', x='age range', y='count', hue='synth round', ci=None,
                        palette='dark', alpha=.6, height=6)
        g.despine(left=True)
        g.set_axis_labels("Age range", "Visit count")
        g.legend.set_title("")

        return g


class UpdateVisitsCountChartStep(CSVChartStep):
    """
    Creates a chart for the visits_count.csv results file.
    """

    @property
    def csv_file(self):
        return self.results_path / 'visits_count.csv'

    def create_chart(self, data):
        sns.set_theme(style='whitegrid')
        g = sns.catplot(data=data, kind='bar', x='synth round', y='count', ci=None, palette='dark',
                        alpha=.6, height=6)
        g.despine(left=True)
        g.set_axis_labels("Synth round", "Visit count")

        return g


class UpdateVisitsGenderChartStep(CSVChartStep):
    """
    Creates a chart for the visits_gender_count.csv results file.
    """

    @property
    def csv_file(self):
        return self.results_path / 'visits_gender_count.csv'

    def create_chart(self, data):
        sns.set_theme(style='whitegrid')
        g = sns.catplot(data=data, kind='bar', x='synth round', y='count', hue='gender', ci=None,
                        palette='dark', alpha=.6, height=6)
        g.despine(left=True)
        g.set_axis_labels("", "Visit count")
        g.legend.set_title("")

        return g
