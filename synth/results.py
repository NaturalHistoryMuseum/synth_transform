import csv

from synth.utils import Step


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
        with self.sql_file.open('rt') as f, self.csv_file.open('w') as g:
            # set the lineterminator for consistency across platforms
            writer = csv.writer(g, lineterminator='\n')
            result = target.execute(f.read())
            writer.writerow(result.keys())
            writer.writerows(result.fetchall())
