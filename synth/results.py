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
