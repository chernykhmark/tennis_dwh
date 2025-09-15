
import os
from pathlib import Path

from pg_connect import ConnectionBuilder, PgConnect



class SchemaDdl:

    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_schema(self, path_to_scripts: str) -> None:
        files = os.listdir(path_to_scripts)
        file_paths = [Path(path_to_scripts, f) for f in files]
        file_paths.sort(key=lambda x: x.name)

        i = 1
        for fp in file_paths:
            script = fp.read_text()
            print(f'reading {fp}')

            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(script)
                    print(f'executing')

            i += 1

        print(f'all scripts done')

if __name__=='__main__':
    SchemaDdl(ConnectionBuilder.pg_conn()).init_schema('../../scripts/ddl')