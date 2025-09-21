

import logging
from logging import Logger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Консоль
        logging.FileHandler('../tests/progress.log')  # Файл
    ]
)


from typing import List, Any, Dict
from datetime import datetime

from lib.service_tables import StgEtlSettingsRepository, EtlSetting

from lib.pg_connect import ConnectionBuilder,PgConnect
from lib.mongo_connect import MongoConnect

from lib.dict_util import json2str
from psycopg import Connection


class OriginReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc

    def get_matches(self, load_threshold: datetime, limit: int) -> List[Dict]:
        with self.dbs.get_db("matches") as collection:
            filter_query = {'processed_at': {'$gt': load_threshold}, 'status': 'processed'}
            docs = list(collection.find(filter=filter_query, limit=limit))
            return docs




class PgSaver:

    def save_object(self, conn: Connection, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.mongo_matches(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )




class MatchesLoader:
    _LOG_THRESHOLD = 10
    _SESSION_LIMIT = 10000

    WF_KEY = "mongo_matches_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_processed_ts"

    def __init__(self, collection_loader: OriginReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2025, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_matches(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from matches collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d["processed_at"], d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing matches.")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["processed_at"] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)






def load_matches():
    log = logging.getLogger(__name__)
    collection_reader = OriginReader(MongoConnect())

    loader = MatchesLoader(collection_reader, ConnectionBuilder.pg_conn(), PgSaver(), log)

    loader.run_copy()


if __name__=='__main__':
    load_matches()