from baskervillehall.settings import Settings
import psycopg2


class SettingsPostgres(Settings):

    def __init__(self,
                 postgres_connection=None,
                 refresh_period_in_seconds=180,
                 ):
        super().__init__(
            refresh_period_in_seconds=refresh_period_in_seconds
        )
        self.postgres_connection = postgres_connection

    def read(self):
        if len(self.postgres_connection['host']) == 0:
            return

        connection = psycopg2.connect(**self.postgres_connection)

        cursor = connection.cursor()
        cursor.execute('select hostname, 0 as sensitivity from hostname '
                       'inner join settings using(hostname_id);')
        self.settings = dict()
        for r in cursor.fetchall():
            self.settings[r[0]] = {
                'sensitivity': r[1],
            }
