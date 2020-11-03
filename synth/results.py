import abc
import csv

import pandas as pd
import seaborn as sns

from synth.utils import Step, SynthRound


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

    def load(self):
        return pd.read_csv(self.csv_file)

    def save(self, chart, path=None):
        if path is None:
            path = self.results_path / f'{self.csv_file.stem}.png'
        chart.savefig(path)


class UpdateVisitsAgeRangeChartStep(CSVChartStep):
    """
    Creates a chart for the visits_age_range_count.csv results file.
    """

    @property
    def csv_file(self):
        return self.results_path / 'visits_age_range_count.csv'

    def run(self, context, target, *args, **kwargs):
        sns.set_theme(style='whitegrid')
        g = sns.catplot(data=self.load(), kind='bar', x='age range', y='count', hue='synth round',
                        ci=None, palette='dark', alpha=.6, height=6)
        g.despine(left=True)
        g.set_axis_labels("Age range", "Visit count")
        g.legend.set_title("")

        self.save(g)


class UpdateVisitsCountChartStep(CSVChartStep):
    """
    Creates a chart for the visits_count.csv results file.
    """

    @property
    def csv_file(self):
        return self.results_path / 'visits_count.csv'

    def run(self, context, target, *args, **kwargs):
        sns.set_theme(style='whitegrid')
        g = sns.catplot(data=self.load(), kind='bar', x='synth round', y='count', ci=None,
                        palette='dark', alpha=.6, height=6)
        g.despine(left=True)
        g.set_axis_labels("Synth round", "Visit count")

        self.save(g)


class UpdateVisitsGenderChartStep(CSVChartStep):
    """
    Creates a chart for the visits_gender_count.csv results file.
    """

    @property
    def csv_file(self):
        return self.results_path / 'visits_gender_count.csv'

    def run(self, context, target, *args, **kwargs):
        sns.set_theme(style='whitegrid')
        g = sns.catplot(data=self.load(), kind='bar', x='synth round', y='count', hue='gender',
                        ci=None,
                        palette='dark', alpha=.6, height=6)
        g.despine(left=True)
        g.set_axis_labels("", "Visit count")
        g.legend.set_title("")

        self.save(g)


class UpdateVisitorNationalityCountStep(CSVChartStep):
    """
    Creates charts for the visits_visitor_nationality_count.csv results file.
    """

    @property
    def csv_file(self):
        return self.results_path / 'visits_visitor_nationality_count.csv'

    def run(self, context, target, *args, **kwargs):
        data = self.load()

        # TODO: this chart is silly
        sns.set_theme(style='whitegrid')
        g = sns.catplot(data=data, kind='bar', x='synth round', y='count',
                        hue='visitor nationality country code', ci=None, palette='dark', alpha=.6,
                        height=6)
        g.despine(left=True)
        g.set_axis_labels("", "Visit count")
        g.legend.set_title("Country code")
        self.save(g)

        for synth_round in SynthRound:
            subset = data.loc[data['synth round'] == f'Synthesys {synth_round}']
            g = sns.catplot(data=subset, kind='bar', y='visitor nationality country code',
                            x='count', ci=None, palette='dark', alpha=.6, orient='h', height=8)
            g.despine(left=True)
            g.set_axis_labels("Visit count", "Country code")
            filename = f'visits_visitor_nationality_count_synth_{synth_round}.png'
            self.save(g, path=self.results_path / filename)
