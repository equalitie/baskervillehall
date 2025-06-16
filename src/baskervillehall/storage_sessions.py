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
        s = record
        requests = self.get_session_requests(s, self.num_requests)
        host = s["host"]
        host_id = self.get_host_id(host)
        if len(host_id) == 0:
            return None
        hits = len(s['requests'])
        duration = s['duration']
        if duration < 1:
            duration = 1
        num_ua = self.get_number_of_useragents(s)
        ua = s["ua"].replace("\'", "")
        asn_name = s.get('asn_name', '').replace("\'", "")
        ciphers = ','.join(s["ciphers"])

        return f'insert into {self.table} (\n'\
            f'hostname_id, host_name, ip, session_cookie, ip_cookie, '\
            f'primary_session, human, class, vpn, user_agent, country, continent, '\
            f'datacenter, hits, \n'\
            f'hit_rate, num_user_agent, passed_challenge, bot_score,bot_score_top_factor,'\
            f'duration, session_start, session_end, requests, fingerprints,scraper_name,'\
            f'ua_score,verified_bot,num_languages,valid_browser_ciphers,cipher,ciphers,'\
            f'asn,asn_name,is_scraper'\
            f')\n'\
            f'values (\'{host_id}\', \'{host}\', \'{s["ip"]}\', \'{s["session_id"]}\',\n'\
            f'\'{s["ip"]}_{s["session_id"]}\',{int(s["primary_session"])},\n'\
            f'{int(s["human"])},\'{s["class"]}\',{int(s["vpn"])},'\
            f'\'{ua}\', \n \'{s["country"]}\', \'{s["continent"]}\', '\
            f'\'{s["datacenter_code"]}\',\n'\
            f'{hits}, {hits * 60.0 / duration:.1f}, {num_ua}, '\
            f'{int(s.get("passed_challenge"))},{s.get("bot_score"):2f},' \
            f'\'{s.get("bot_score_top_factor", "")}\',' \
            f'{duration:.1f}, \'{s["start"]}\', \'{s["end"]}\',\n'\
            f'\'{requests}\',\'{s["fingerprints"]}\',\'{s["scraper_name"]}\','\
            f'{s.get("ua_score"):2f},{int(s["verified_bot"])},'\
            f'{int(s["num_languages"])},{int(s["valid_browser_ciphers"])},'\
            f'\'{s["cipher"]}\',\'{ciphers}\','\
            f'{int(s["asn"])},\'{asn_name}\',{int(s["is_scraper"])}'\
            f');'
