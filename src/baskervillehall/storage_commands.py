import json

from baskervillehall.storage_base import StorageBase

class StorageCommands(StorageBase):
    def __init__(
            self,
            topic='COMMANDS',
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
        command = record
        s = command['session']
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
        shapley_formatted = json.dumps(command['shapley'])
        ua = s["ua"].replace("\'", "")
        return f'insert into {self.table} (\n'\
            f'hostname_id, host_name, ip_address, session_cookie, ip_cookie, '\
            f'primary_session, human, passed_challenge, user_agent, country, continent, '\
            f'datacenter, hits, score, bot_score, bot_score_top_factor, shapley_feature, difficulty, shapley, request_count, command_type_name, source, \n'\
            f'meta, hit_rate, num_user_agent,'\
            f'duration, session_start, session_end, requests,updated_by,scraper_name)\n'\
            f'values (\'{host_id}\', \'{host}\', \'{s["ip"]}\', \'{s["session_id"]}\',\n'\
            f'\'{s["ip"]}_{s["session_id"]}\',{int(s["primary_session"])},\n'\
            f'{int(s["human"])},'\
            f'{int(s["passed_challenge"])}, \'{ua}\', \n \'{s["country"]}\','\
            f' \'{s["continent"]}\', '\
            f'\'{s["datacenter_code"]}\',\n'\
            f'{hits}, {command["score"]},{command.get("bot_score", 0.0)},'\
            f'\'{command.get("bot_score_top_factor", "")}\','\
            f'\'{command.get("shapley_feature","")}\','\
            f'{command["difficulty"]},\'{shapley_formatted}\', '\
            f'{command["num_requests"]},\'{command["Name"]}\','\
            f'\'{command["source"]}\',' \
            f'\'{command["meta"]}\',' \
            f'{hits * 60.0 / duration:.1f}, {num_ua}, '\
            f'{duration:.1f}, \'{s["start"]}\', \'{s["end"]}\',\n'\
            f'\'{requests}\', \'pipeline\',\'{s["scraper_name"]}\''\
            f');'
