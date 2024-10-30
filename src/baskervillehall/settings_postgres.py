from baskervillehall.settings import Settings
import psycopg2


class SettingsPostgres(Settings):

    def __init__(self,
                 postgres_host,
                 postgres_user,
                 postgres_password,
                 database_name,
                 postgres_port=5432,
                 refresh_period_in_seconds=180,
                 ):
        super().__init__(
            refresh_period_in_seconds=refresh_period_in_seconds
        )

        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.database_name = database_name

    def read(self):
        if not self.postgres_port or len(self.postgres_host) == 0:
            return

        connection = psycopg2.connect(
            database=self.database_name,
            user=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port)

        cursor = connection.cursor()
        cursor.execute('select hostname, 0 as sensitivity from hostname '
                       'inner join settings using(hostname_id);')
        self.settings = dict()
        for r in cursor.fetchall():
            self.settings[r[0]] = {
                'sensitivity': r[1],
            }
