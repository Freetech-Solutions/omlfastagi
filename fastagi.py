import os
import re
import sys
import time
import logging
import pystrix
import threading
import redis
from redis.maint_notifications import MaintNotificationsConfig
import psycopg2
from psycopg2 import sql

# Configuración del logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stdout_handler.setFormatter(formatter)

root_logger.addHandler(stdout_handler)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

ERR_INSUFFICIENT_ARGS = 'Error: Insufficient arguments provided'


class FastAGIServer(threading.Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True

        interface = os.environ.get('FASTAGI_HOSTNAME', '0.0.0.0')
        self._fagi_server = pystrix.agi.FastAGIServer(interface=interface)

        # Handlers usados por dialplan (oml_extensions.conf)
        self._fagi_server.register_script_handler(
            re.compile('omni-retrieve-conf'), self.omni_retrieve_conf)
        self._fagi_server.register_script_handler(
            re.compile('omni-survey-answer'), self.omni_survey_answer)

    def get_redis_connection(self, db=0):
        # OSS Redis no soporta MAINT_NOTIFICATIONS; evita spam DEBUG de redis-py 8.x
        return redis.Redis(
            host=os.getenv('REDIS_HOSTNAME', 'redis'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=db,
            decode_responses=True,
            maint_notifications_config=MaintNotificationsConfig(enabled=False))

    def omni_retrieve_conf(self, agi, *args, **kwargs):
        arguments = args[0]

        if len(arguments) < 2:
            root_logger.error(ERR_INSUFFICIENT_ARGS)
            return

        family_type, item_id = arguments[:2]
        family_key = f'OML:{family_type}:{item_id}'

        try:
            redis_connection = self.get_redis_connection(db=0)
            family_data = redis_connection.hgetall(family_key)
            if not family_data:
                root_logger.error("Unable to get Family DATA for %s", family_key)
                return

            for key, value in family_data.items():
                variable_name = f'__OML{family_type}{key}'
                agi.execute(pystrix.agi.core.SetVariable(variable_name, value))
        except redis.exceptions.RedisError as e:
            root_logger.error(
                "Error executing Redis command HGETALL for %s: %s", family_key, e)
        except Exception as e:
            root_logger.error("Unable to set variable in channel due to %s", e)
            raise e

    def omni_survey_answer(self, agi, *args, **kwargs):
        if not args or len(args[0]) != 9:
            root_logger.error(
                "Error: Argumentos inesperados omni_survey_answer. "
                "Se esperaban 9 argumentos.")
            return

        respuesta = args[0]
        respuesta_dict = {
            'campana_id': respuesta[0],
            'pregunta_id': respuesta[1],
            'opcion_id': None if respuesta[2] == '-1' else respuesta[2],
            'fecha': respuesta[3],
            'callid': respuesta[4],
            'callerid': respuesta[5],
            'contacto_id': None if respuesta[6] == '-1' else respuesta[6],
            'agente_id': None if respuesta[7] == '-1' else respuesta[7],
            'grabacion': None if respuesta[8] == '-1' else respuesta[8],
        }

        try:
            with psycopg2.connect(
                    host=os.getenv('PGHOST'),
                    port=os.getenv('PGPORT'),
                    dbname=os.getenv('PGDATABASE'),
                    user=os.getenv('PGUSER'),
                    password=os.getenv('PGPASSWORD')) as conn:
                with conn.cursor() as cursor:
                    insert_query = sql.SQL(
                        'INSERT INTO survey_app_respuestadepreguntadeencuesta ({}) VALUES ({})'
                    ).format(
                        sql.SQL(',').join(map(sql.Identifier, respuesta_dict.keys())),
                        sql.SQL(',').join(map(sql.Placeholder, respuesta_dict.keys()))
                    )
                    cursor.execute(insert_query, respuesta_dict)
                    conn.commit()
        except Exception as e:
            root_logger.error('Error inserting survey data into DB: %s', e)

    def kill(self):
        self._fagi_server.shutdown()

    def run(self):
        self._fagi_server.serve_forever()


# Bloque principal para iniciar el servidor
if __name__ == '__main__':
    fastagi_core = FastAGIServer()
    fastagi_core.start()

    try:
        while fastagi_core.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        fastagi_core.kill()
