from baskervillehall.storage_base import StorageBase


class StorageSessions(StorageBase):
    def __init__(
            self,
            topic='SESSIONS',
            partition=0,
            batch_size=100,
            kafka_connection=None,
            datetime_format='%Y-%m-%d %H:%M:%S',
            ttl_records_days=7,
            logger=None,
            postgres_connection=None,
            num_requests=20,
            table=None,
            autocreate_hostname_id=True
    ):
        super().__init__(
            topic=topic,
            partition=partition,
            batch_size=batch_size,
            kafka_connection=kafka_connection,
            datetime_format=datetime_format,
            ttl_records_days=ttl_records_days,
            logger=logger,
            postgres_connection=postgres_connection,
            table=table,
            autocreate_hostname_id=autocreate_hostname_id
        )
        self.num_requests = num_requests

    def get_sql(self, record):
        session = record
        requests = self.get_session_requests(session, self.num_requests)
        host = session["host"]
        host_id = self.get_host_id(host)
        if len(host_id) == 0:
            return None
        hits = len(session['requests'])
        duration = session['duration']
        if duration < 1:
            duration = 1
        num_ua = self.get_number_of_useragents(session)
        ua = session["ua"].replace("\'", "")
        return f'insert into {self.table} (\n'\
            f'hostname_id, host_name, ip, session_cookie, ip_cookie, '\
            f'primary_session, human, class, vpn, user_agent, country, continent, '\
            f'datacenter, hits, \n'\
            f'hit_rate, num_user_agent,'\
            f'duration, session_start, session_end, requests)\n'\
            f'values (\'{host_id}\', \'{host}\', \'{session["ip"]}\', \'{session["session_id"]}\',\n'\
            f'\'{session["ip"]}_{session["session_id"]}\',{int(session["primary_session"])},\n'\
            f'{int(session["human"])},\'{session["class"]}\',{int(session["vpn"])},'\
            f'\'{ua}\', \n \'{session["country"]}\', \'{session["continent"]}\', '\
            f'\'{session["datacenter_code"]}\',\n'\
            f'{hits}, {hits * 60.0 / duration:.1f}, {num_ua}, '\
            f'{duration:.1f}, \'{session["start"]}\', \'{session["end"]}\',\n'\
            f'\'{requests}\''\
            f');'
