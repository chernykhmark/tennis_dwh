

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

from lib.service_tables import OdsEtlSettingsRepository, EtlSetting

from lib.pg_connect import ConnectionBuilder,PgConnect

from lib.dict_util import json2str
from psycopg import Connection

from scripts.parser.ods_safe_extract import add_data_to_ods


class PgWorker:

    def get_objects(self, conn: Connection, last_loaded_ts):
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT
                        object_value::JSON->>'scheduled_html' as shed,
                        object_value::JSON->>'finished_html' as fin, 
                        object_value::JSON->>'result_score' as result_score,
                        object_value::JSON->>'game_id' as game_id,
                        update_ts
                    FROM stg.mongo_matches
                    WHERE update_ts > %(trashold)s
                    ORDER BY update_ts 
                """,
                {
                    "trashold": last_loaded_ts
                }
            )
            obj = cur.fetchall()
        return obj

    def save_object(self, conn: Connection, data: Any):
        with conn.cursor() as cur:
            cur.execute("""
                    INSERT INTO ods.fct_matches (
                        game_id, tour, round, datetime, player_id_home, player_link_home, player_name_home, player_rank_home,
                        player_id_away, player_link_away, player_name_away, player_rank_away, player_odd_home, player_odd_away,
                        wins_in_last_5_games_home, wins_in_last_5_games_away, wins_in_h2h_home, wins_in_h2h_away,
                        is_winner_home, is_winner_away, result_score_home, result_score_away,
                        aces_home, aces_away, double_faults_home, double_faults_away,
                        first_serve_percentage_home, first_serve_percentage_away,
                        first_serve_points_won_home, first_serve_points_total_home,
                        first_serve_points_won_away, first_serve_points_total_away,
                        second_serve_points_won_home, second_serve_points_total_home,
                        second_serve_points_won_away, second_serve_points_total_away,
                        break_points_saved_home, break_points_total_home,
                        break_points_saved_away, break_points_total_away,
                        first_return_points_won_home, first_return_points_total_home,
                        first_return_points_won_away, first_return_points_total_away,
                        second_return_points_won_home, second_return_points_total_home,
                        second_return_points_won_away, second_return_points_total_away,
                        break_points_converted_home, break_points_converted_total_home,
                        break_points_converted_away, break_points_converted_total_away,
                        service_points_won_home, service_points_total_home,
                        service_points_won_away, service_points_total_away,
                        return_points_won_home, return_points_total_home,
                        return_points_won_away, return_points_total_away,
                        total_points_won_home, total_points_total_home,
                        total_points_won_away, total_points_total_away,
                        last_10_balls_won_home, last_10_balls_won_away,
                        match_points_saved_home, match_points_saved_away,
                        service_games_won_home, service_games_total_home,
                        service_games_won_away, service_games_total_away,
                        return_games_won_home, return_games_total_home,
                        return_games_won_away, return_games_total_away,
                        total_games_won_home, total_games_total_home,
                        total_games_won_away, total_games_total_away
                    ) VALUES (
                        %(game_id)s, %(tour)s, %(round)s, %(datetime)s, %(player_id_home)s, %(player_link_home)s, 
                        %(player_name_home)s, %(player_rank_home)s, %(player_id_away)s, %(player_link_away)s, 
                        %(player_name_away)s, %(player_rank_away)s, %(player_odd_home)s, %(player_odd_away)s,
                        %(wins_in_last_5_games_home)s, %(wins_in_last_5_games_away)s, %(wins_in_h2h_home)s, %(wins_in_h2h_away)s,
                        %(is_winner_home)s, %(is_winner_away)s, %(result_score_home)s, %(result_score_away)s,
                        %(aces_home)s, %(aces_away)s, %(double_faults_home)s, %(double_faults_away)s,
                        %(first_serve_percentage_home)s, %(first_serve_percentage_away)s,
                        %(first_serve_points_won_home)s, %(first_serve_points_total_home)s,
                        %(first_serve_points_won_away)s, %(first_serve_points_total_away)s,
                        %(second_serve_points_won_home)s, %(second_serve_points_total_home)s,
                        %(second_serve_points_won_away)s, %(second_serve_points_total_away)s,
                        %(break_points_saved_home)s, %(break_points_total_home)s,
                        %(break_points_saved_away)s, %(break_points_total_away)s,
                        %(first_return_points_won_home)s, %(first_return_points_total_home)s,
                        %(first_return_points_won_away)s, %(first_return_points_total_away)s,
                        %(second_return_points_won_home)s, %(second_return_points_total_home)s,
                        %(second_return_points_won_away)s, %(second_return_points_total_away)s,
                        %(break_points_converted_home)s, %(break_points_converted_total_home)s,
                        %(break_points_converted_away)s, %(break_points_converted_total_away)s,
                        %(service_points_won_home)s, %(service_points_total_home)s,
                        %(service_points_won_away)s, %(service_points_total_away)s,
                        %(return_points_won_home)s, %(return_points_total_home)s,
                        %(return_points_won_away)s, %(return_points_total_away)s,
                        %(total_points_won_home)s, %(total_points_total_home)s,
                        %(total_points_won_away)s, %(total_points_total_away)s,
                        %(last_10_balls_won_home)s, %(last_10_balls_won_away)s,
                        %(match_points_saved_home)s, %(match_points_saved_away)s,
                        %(service_games_won_home)s, %(service_games_total_home)s,
                        %(service_games_won_away)s, %(service_games_total_away)s,
                        %(return_games_won_home)s, %(return_games_total_home)s,
                        %(return_games_won_away)s, %(return_games_total_away)s,
                        %(total_games_won_home)s, %(total_games_total_home)s,
                        %(total_games_won_away)s, %(total_games_total_away)s
                    ) ON CONFLICT (game_id) DO NOTHING;
                """, {
                "game_id": data['game_id'],
                "tour": data['tour'],
                "round": data['round'],
                "datetime": data['datetime'],
                "player_id_home": data['player_id_home'],
                "player_link_home": data['player_link_home'],
                "player_name_home": data['player_name_home'],
                "player_rank_home": data['player_rank_home'],
                "player_id_away": data['player_id_away'],
                "player_link_away": data['player_link_away'],
                "player_name_away": data['player_name_away'],
                "player_rank_away": data['player_rank_away'],
                "player_odd_home": data['player_odd_home'],
                "player_odd_away": data['player_odd_away'],
                "wins_in_last_5_games_home": data['wins_in_last_5_games_home'],
                "wins_in_last_5_games_away": data['wins_in_last_5_games_away'],
                "wins_in_h2h_home": data['wins_in_h2h_home'],
                "wins_in_h2h_away": data['wins_in_h2h_away'],
                "is_winner_home": data['is_winner_home'],
                "is_winner_away": data['is_winner_away'],
                "result_score_home": json2str(data['result_score_home']),
                "result_score_away": json2str(data['result_score_away']),
                "aces_home": data['aces_home'],
                "aces_away": data['aces_away'],
                "double_faults_home": data['double_faults_home'],
                "double_faults_away": data['double_faults_away'],
                "first_serve_percentage_home": data['first_serve_percentage_home'],
                "first_serve_percentage_away": data['first_serve_percentage_away'],
                "first_serve_points_won_home": data['first_serve_points_won_home'],
                "first_serve_points_total_home": data['first_serve_points_total_home'],
                "first_serve_points_won_away": data['first_serve_points_won_away'],
                "first_serve_points_total_away": data['first_serve_points_total_away'],
                "second_serve_points_won_home": data['second_serve_points_won_home'],
                "second_serve_points_total_home": data['second_serve_points_total_home'],
                "second_serve_points_won_away": data['second_serve_points_won_away'],
                "second_serve_points_total_away": data['second_serve_points_total_away'],
                "break_points_saved_home": data['break_points_saved_home'],
                "break_points_total_home": data['break_points_total_home'],
                "break_points_saved_away": data['break_points_saved_away'],
                "break_points_total_away": data['break_points_total_away'],
                "first_return_points_won_home": data['first_return_points_won_home'],
                "first_return_points_total_home": data['first_return_points_total_home'],
                "first_return_points_won_away": data['first_return_points_won_away'],
                "first_return_points_total_away": data['first_return_points_total_away'],
                "second_return_points_won_home": data['second_return_points_won_home'],
                "second_return_points_total_home": data['second_return_points_total_home'],
                "second_return_points_won_away": data['second_return_points_won_away'],
                "second_return_points_total_away": data['second_return_points_total_away'],
                "break_points_converted_home": data['break_points_converted_home'],
                "break_points_converted_total_home": data['break_points_converted_total_home'],
                "break_points_converted_away": data['break_points_converted_away'],
                "break_points_converted_total_away": data['break_points_converted_total_away'],
                "service_points_won_home": data['service_points_won_home'],
                "service_points_total_home": data['service_points_total_home'],
                "service_points_won_away": data['service_points_won_away'],
                "service_points_total_away": data['service_points_total_away'],
                "return_points_won_home": data['return_points_won_home'],
                "return_points_total_home": data['return_points_total_home'],
                "return_points_won_away": data['return_points_won_away'],
                "return_points_total_away": data['return_points_total_away'],
                "total_points_won_home": data['total_points_won_home'],
                "total_points_total_home": data['total_points_total_home'],
                "total_points_won_away": data['total_points_won_away'],
                "total_points_total_away": data['total_points_total_away'],
                "last_10_balls_won_home": data['last_10_balls_won_home'],
                "last_10_balls_won_away": data['last_10_balls_won_away'],
                "match_points_saved_home": data['match_points_saved_home'],
                "match_points_saved_away": data['match_points_saved_away'],
                "service_games_won_home": data['service_games_won_home'],
                "service_games_total_home": data['service_games_total_home'],
                "service_games_won_away": data['service_games_won_away'],
                "service_games_total_away": data['service_games_total_away'],
                "return_games_won_home": data['return_games_won_home'],
                "return_games_total_home": data['return_games_total_home'],
                "return_games_won_away": data['return_games_won_away'],
                "return_games_total_away": data['return_games_total_away'],
                "total_games_won_home": data['total_games_won_home'],
                "total_games_total_home": data['total_games_total_home'],
                "total_games_won_away": data['total_games_won_away'],
                "total_games_total_away": data['total_games_total_away']
            })


class OdsLoader:
    _LOG_THRESHOLD = 10
    _SESSION_LIMIT = 10000

    WF_KEY = "stg_ods_loader_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg_dest: PgConnect, pg_worker: PgWorker, logger: Logger) -> None:
        self.pg_worker = pg_worker
        self.pg_dest = pg_dest
        self.settings_repository = OdsEtlSettingsRepository()
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

            load_queue = self.pg_worker.get_objects(conn,last_loaded_ts)

            self.log.info(f"Found {len(load_queue)} matches to sync from staging.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for obj in load_queue:
                data = add_data_to_ods(obj)
                if data:
                    try:
                        self.pg_worker.save_object(conn, data)
                    except Exception as e:
                        self.log.error(f"{e} for {data}")
                        continue

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing matches.")


            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t[-1] for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)


def load_to_ods():
    log = logging.getLogger(__name__)

    loader = OdsLoader(ConnectionBuilder.pg_conn(), PgWorker(), log)

    loader.run_copy()


if __name__=='__main__':
    load_to_ods()