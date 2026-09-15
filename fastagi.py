import os
import re
import sys
import time
import datetime
import pytz
import logging
import pystrix
import threading
import redis
from redis.maint_notifications import MaintNotificationsConfig
import psycopg2
from psycopg2 import sql
import json
import requests
import urllib3
import gearman

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

CALLDATA_CAMP_KEY = 'OML:CALLDATA:CAMP:{0}'
CALLDATA_AGENT_KEY = 'OML:CALLDATA:AGENT:{0}'
CALLDATA_WAIT_KEY = 'OML:CALLDATA:WAIT-TIME:CAMP:{0}'
CALLEVENTS_CHANNEL = 'OML:CHANNEL:CALLEVENTS'
ERR_INSUFFICIENT_ARGS = 'Error: Insufficient arguments provided'


class FastAGIServer(threading.Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True

        interface = os.environ.get('FASTAGI_HOSTNAME', '0.0.0.0')
        self._fagi_server = pystrix.agi.FastAGIServer(interface=interface)

        # Registro de manejadores de scripts
        self._fagi_server.register_script_handler(
            re.compile('variables'), self.variables)
        self._fagi_server.register_script_handler(
            re.compile('channel-variables'), self.channel_variables)
        self._fagi_server.register_script_handler(
            re.compile('omni-retrieve-conf'), self.omni_retrieve_conf)
        self._fagi_server.register_script_handler(
            re.compile('omni-retrieve-value'), self.omni_retrieve_value)
        self._fagi_server.register_script_handler(
            re.compile('omni-survey-answer'), self.omni_survey_answer)
        self._fagi_server.register_script_handler(
            re.compile('omni-crm-customer-vars'),
            self.set_asterisk_channel_variables_from_api)
        self._fagi_server.register_script_handler(
            re.compile('omni-conference-ids'), self.omni_conference_ids)
        self._fagi_server.register_script_handler(
            re.compile('omni-dial2multinum'),
            self.omni_dial2multinum)
        self._fagi_server.register_script_handler(
            re.compile('omni-notify-multinum-answer'),
            self.omni_notify_multinum_answer)
        self._fagi_server.register_script_handler(
            re.compile('omni-amd'),
            self.omni_amd)

    def variables(self, agi, *args, **kwargs):
        argumento = args[0]
        agi.execute(pystrix.agi.core.SetVariable(
            'DESCRIPCION', 'Mostramos variables'))
        agi.execute(pystrix.agi.core.SetVariable('DESTINO', '102'))
        agi.execute(pystrix.agi.core.SetVariable('TIEMPO', '10'))
        agi.execute(pystrix.agi.core.SetVariable('OPCIONES', 'TtR'))
        agi.execute(pystrix.agi.core.SetVariable('ARGUMENTO1', argumento[0]))
        agi.execute(pystrix.agi.core.SetVariable('ARGUMENTO2', argumento[1]))
        agi.execute(pystrix.agi.core.SetVariable('ARGUMENTO3', argumento[2]))

    def channel_variables(self, agi, *args, **kwargs):
        agi.execute(pystrix.agi.core.SetVariable(
            'CHANVAR2',
            agi.execute(pystrix.agi.core.GetFullVariable('${EXTEN}'))))
        agi.execute(pystrix.agi.core.SetVariable(
            'CHANVAR1',
            agi.execute(pystrix.agi.core.GetFullVariable('${CONTEXT}'))))
        agi.execute(pystrix.agi.core.SetVariable(
            'CHANVAR3',
            agi.execute(pystrix.agi.core.GetFullVariable('${PRIORITY}'))))
        agi.execute(pystrix.agi.core.SetVariable(
            'CHANVAR4',
            agi.execute(pystrix.agi.core.GetFullVariable('${CHANNEL}'))))
        agi.execute(pystrix.agi.core.SetVariable(
            'CHANVAR5',
            agi.execute(pystrix.agi.core.GetFullVariable(
                '${CALLERID(all)}'))))

    def get_date(self, formato="%Y-%m-%d %H:%M:%S"):
        time_zone = os.getenv('TZ', 'UTC')
        tz = pytz.timezone(time_zone)
        now = datetime.datetime.now(tz)
        return now.strftime(formato)

    def get_redis_connection(self, db=0):
        # OSS Redis no soporta MAINT_NOTIFICATIONS; evita spam DEBUG de redis-py 8.x
        return redis.Redis(
            host=os.getenv('REDIS_HOSTNAME', 'redis'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=db,
            decode_responses=True,
            maint_notifications_config=MaintNotificationsConfig(enabled=False))

    def _notify_calldata_event(self, event_data):
        try:
            redis_connection = self.get_redis_connection()
            redis_connection.publish(CALLEVENTS_CHANNEL, json.dumps(event_data))
        except redis.exceptions.RedisError as e:
            root_logger.error("Redis _notify_calldata_event error: %s", e)

    # Redis events CAMP INCRDB
    def _record_camp_calldata_event(self, campana_id, tipo_llamada, event):
        redis_key = CALLDATA_CAMP_KEY.format(campana_id)
        field = f'CALL_TYPE:{tipo_llamada}:{event}'
        try:
            redis_connection = self.get_redis_connection(db=2)
            redis_connection.hincrby(redis_key, field, 1)
            self._notify_calldata_event({'type': 'CAMP',
                                         'id': campana_id,
                                         'event': event,
                                         'call_type': tipo_llamada})
        except redis.exceptions.RedisError as e:
            root_logger.error("Redis _record_camp_calldata_event error: %s", e)

    def _record_attended_call_wait_time(self, campana_id, agente_id, wait_time, event):
        if agente_id is None or agente_id == '-1':
            return
        EVENTOS_FIN_CONEXION_ORIGINAL = [
            'COMPLETEAGENT', 'COMPLETEOUTNUM', 'BT-TRY', 'BTOUT-TRY',
            'CAMPT-COMPLETE', 'CAMPT-FAIL', 'COMPLETE-CAMPT', 'CT-COMPLETE', 'CTOUT-COMPLETE'
        ]
        if event in EVENTOS_FIN_CONEXION_ORIGINAL:
            redis_key = CALLDATA_WAIT_KEY.format(campana_id)
            try:
                redis_connection = self.get_redis_connection(db=2)
                redis_connection.rpush(redis_key, wait_time)
                self._notify_calldata_event({'type': 'WAIT',
                                             'id': campana_id,
                                             'time': wait_time})
            except redis.exceptions.RedisError as e:
                root_logger.error("Redis _record_attended_call_wait_time error: %s", e)

    def omni_retrieve_conf(self, agi, *args, **kwargs):
        arguments = args[0]

        if len(arguments) < 2:
            root_logger.error(ERR_INSUFFICIENT_ARGS)
            return

        family_type, item_id = arguments[:2]
        family_key = f'OML:{family_type}:{item_id}'

        redis_connection = self.get_redis_connection(db=0)

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

    def omni_retrieve_value(self, agi, *args, **kwargs):
        """
        Recupera un campo específico de un hash en Redis y lo establece como variable de canal.

        Esta función es similar a omni_retrieve_conf, pero solo recupera un campo específico
        del hash en lugar de todo el hash.

        Args:
            agi: Objeto AGI para interactuar con Asterisk
            args: Argumentos pasados desde el dialplan
                  Formato 1 (2 argumentos): hash_field, item_id
                    - hash_field: nombre del campo del hash a recuperar (ej: 'AMD')
                    - item_id: id de la campaña (ej: ${OMLCAMPID})
                    - family_type se asume como 'CAMP' por defecto
                    Ejemplo: AGI(agi://.../omni-retrieve-value,AMD,${OMLCAMPID})

                  Formato 2 (3 argumentos): family_type, item_id, hash_field
                    - family_type: tipo de familia (ej: 'CAMP', 'AMD')
                    - item_id: id del item (ej: ${OMLCAMPID})
                    - hash_field: nombre del campo del hash a recuperar (ej: 'AMD')
                    Ejemplo: AGI(agi://.../omni-retrieve-value,CAMP,${OMLCAMPID},AMD)
        """
        arguments = args[0]

        if len(arguments) < 2:
            root_logger.error(
                "Error: Insufficient arguments provided. Expected: "
                "hash_field, item_id or family_type, item_id, hash_field")
            return

        # Determinar el formato basado en el número de argumentos
        if len(arguments) == 2:
            # Formato 1: hash_field, item_id (family_type = 'CAMP' por defecto)
            hash_field, item_id = arguments[:2]
            family_type = 'CAMP'
        else:
            # Formato 2: family_type, item_id, hash_field
            family_type, item_id, hash_field = arguments[:3]

        family_key = f'OML:{family_type}:{item_id}'

        redis_connection = self.get_redis_connection(db=0)

        try:
            # Obtener solo el campo específico del hash usando HGET
            value = redis_connection.hget(family_key, hash_field)

            if value is None:
                root_logger.error(
                    "Unable to get field '%s' from hash '%s'", hash_field, family_key)
                return

            # Establecer la variable de canal siguiendo el mismo patrón que omni_retrieve_conf
            # Para el formato con CAMP, la variable será __OMLCAMP{hash_field}
            variable_name = f'__OML{family_type}{hash_field}'
            agi.execute(pystrix.agi.core.SetVariable(variable_name, value))
            root_logger.debug(
                "Set variable %s = %s from hash %s field %s",
                variable_name, value, family_key, hash_field)
        except redis.exceptions.RedisError as e:
            root_logger.error(
                "Error executing Redis command HGET for %s field %s: %s",
                family_key, hash_field, e)
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

    def set_asterisk_channel_variables_from_api(self, agi, *args, **kwargs):
        if len(args) < 2:  # Revisar que hay al menos dos argumentos
            root_logger.error(ERR_INSUFFICIENT_ARGS)
            return

        # Verificar si los argumentos son tuplas, y extraer los valores si es necesario
        url = args[0][0]
        codcli = args[0][1]

        # Asegurarse de que 'url' y 'codcli' sean cadenas
        if isinstance(url, tuple):
            url = url[0]
        if isinstance(codcli, tuple):
            codcli = codcli[0]

        # Concatenar URL y codcli correctamente
        url_codcli = f"{url}/{codcli}/"

        try:
            # Realizar la solicitud a la URL concatenada con codcli
            response = requests.get(url_codcli)
            response.raise_for_status()

            # Obtener los datos de la respuesta
            data = response.json()

            # Obtener el valor de 'response' del JSON y establecer la variable de canal
            crm_choice = data.get('response', 'Unknown')
            agi.execute(
                pystrix.agi.core.SetVariable('__CRMCHOICE', crm_choice)
            )

            if crm_choice:  # Si 'response' es True
                # Obtener el diccionario de datos
                data_dict = data.get('data', {})

                # Escapar comillas y barras para evitar conflictos con Asterisk
                escaped_data = json.dumps(data_dict).replace('\\', '\\\\').replace('"', '\\"')

                # Establecer la variable de canal `OML_EXTERNAL_CRM` con los datos del CRM
                agi.execute(
                    pystrix.agi.core.SetVariable('__OML_EXTERNAL_CRM', escaped_data)
                )
            else:
                root_logger.error("API response was not successful: %s", data)
        except requests.exceptions.RequestException as e:
            root_logger.error("Error in HTTP request: %s", e)
        except Exception as e:
            root_logger.error("Unexpected error: %s", e)
            raise e

    def omni_conference_ids(self, agi, *args, **kwargs):
        arguments = args[0]
        if len(arguments) < 2:
            root_logger.error(ERR_INSUFFICIENT_ARGS)
            return

        command, conference_id = arguments[:2]
        conference_key = f'OML:CONFERENCE:{conference_id}'

        redis_connection = self.get_redis_connection()

        if command not in ['SET']:
            root_logger.error("Unknown command %s", command)
            return

        try:
            if command == 'SET':
                data = {
                    'CONF_PSTN1': arguments[2],
                    'CONF_PSTN2': arguments[3],
                }
                redis_connection.hset(conference_key, mapping=data)
                redis_connection.expire(conference_key, 3600)
        except redis.exceptions.RedisError as e:
            root_logger.error("Error executing Redis command: %s", e)

    def omni_dial2multinum(self, agi, *args, **kwargs):
        # Obtener la variable EXTEN
        exten = agi.execute(pystrix.agi.core.GetFullVariable('${EXTEN}'))

        # Dividir la cadena en un array
        multinum_array = exten.split('_')

        # Guardar el array en la variable MULTINUM
        agi.execute(pystrix.agi.core.SetVariable('__MULTINUM', exten))

        # Recorrer el array y crear variables EXTEN_i
        for i, value in enumerate(multinum_array):
            # Crear la variable EXTEN_i (EXTEN_0, EXTEN_1, etc.)
            variable_name = f'EXTEN_{i}'
            agi.execute(pystrix.agi.core.SetVariable(variable_name, value))

        # Crear la variable MULTINUM_COUNT con la cantidad de elementos
        multinum_count = len(multinum_array)
        agi.execute(pystrix.agi.core.SetVariable('MULTINUM_COUNT', str(multinum_count)))

    def omni_notify_multinum_answer(self, agi, *args, **kwargs):
        """
        Notifica a OmniLeads el numero de tel de la llamada multinumero que fue atendida.

        Args:
            agi: Objeto AGI para interactuar con Asterisk
            args: Argumentos pasados desde el dialplan (agent_id, phone)
            kwargs: Argumentos adicionales
        """
        try:
            # Obtener la URL base del entorno
            base_url = os.environ.get('OMNILEADS_HOSTNAME', 'localhost')

            # Construir la URL completa
            url = f'https://{base_url}/api/v1/asterisk/notify_attended_multinum_call/'

            # Extraer correctamente los argumentos
            arguments = args[0]
            if len(arguments) < 2:
                root_logger.error("Error: Insufficient arguments for omni_notify_multinum_answer")
                return

            data = {
                'agent_id': arguments[0],
                'phone': arguments[1]
            }

            # Realizar la solicitud POST con manejo de errores
            root_logger.debug(f"Sending notification to {url} with data: {data}")
            response = requests.post(url, data=data, verify=False, timeout=5)
            response.raise_for_status()

            # Registrar la respuesta
            root_logger.debug(f"Notification successful: {response.status_code}")

        except requests.exceptions.RequestException as e:
            root_logger.error(f"Error en la solicitud HTTP a {url}: {e}")
        except Exception as e:
            root_logger.error(f"Error inesperado en omni_notify_multinum_answer: {e}")

    def omni_amd(self, agi, *args, **kwargs):
        """
        Lee ${CALLERID(name)}, parsea idCamp, idContact y telContact,
        y hace un POST a:
        https://{DIALER_AMD_ENDPOINT}/add_amd_event/
        con el JSON:
        {
            "id_contact": 3,
            "id_campaign": 7,
            "phone_number": "123456789"
        }
        También envía un evento EXIT_AMD a Gearman para cerrar la llamada
        en la tabla reportes_app_llamada_resumen.
        """

        # 1. Leer CallerIDName
        try:
            res = agi.execute(pystrix.agi.core.GetFullVariable('${CALLERID(name)}'))
            caller_name = res.get('value') if isinstance(res, dict) else res
        except Exception as e:
            root_logger.error("Error leyendo ${CALLERID(name)}: %s", e)
            return

        if not caller_name:
            root_logger.error("CallerIDName vacío, no se puede parsear")
            return

        # 2. Parsear "10_5_123456751"
        m = re.match(r'^(\d+)_(\d+)_(\d+)$', caller_name)
        if not m:
            root_logger.error(
                "CallerIDName '%s' no coincide con patrón idCamp_idContact_telContact",
                caller_name
            )
            return

        idCamp, idContact, telContact = m.groups()
        root_logger.info(
            "Parsed CallerIDName → idCamp=%s, idContact=%s, telContact=%s",
            idCamp, idContact, telContact
        )

        # 3. Obtener callid y uniqueid del canal
        callid = None
        uniqueid = None
        try:
            res_callid = agi.execute(pystrix.agi.core.GetFullVariable('${CHANNEL(callid)}'))
            callid = res_callid.get('value') if isinstance(res_callid, dict) else res_callid
            if callid:
                callid = callid.strip()
        except Exception as e:
            root_logger.warning("Error obteniendo callid: %s", e)

        try:
            res_uniqueid = agi.execute(pystrix.agi.core.GetFullVariable('${UNIQUEID}'))
            uniqueid = res_uniqueid.get('value') if isinstance(res_uniqueid, dict) else res_uniqueid
            if uniqueid:
                uniqueid = uniqueid.strip()
        except Exception as e:
            root_logger.warning("Error obteniendo uniqueid: %s", e)

        # 4. Enviar evento EXIT_AMD a Gearman
        try:
            servers_env = os.getenv("GEARMAN_JOB_SERVERS", "gearman:4730")
            gearman_servers = servers_env.split(",")
            gm_client = gearman.GearmanClient(gearman_servers)

            # Construir payload para Gearman
            timestamp_iso = datetime.datetime.now(pytz.UTC).isoformat()
            gearman_payload = {
                'event': 'EXIT_AMD',
                'callid': callid,
                'uniqueid': uniqueid,
                'campana_id': int(idCamp),
                'contacto_id': int(idContact),
                'numero_marcado': telContact,
                'tipo_llamada': 5,  # dialer según TIPO_LLAMADA_MAPPING
                'tipo_campana': 2,  # dialer según TIPO_CAMPANA_MAPPING
                'time': timestamp_iso
            }

            # Enviar mensaje a Gearman (no bloqueante)
            json_payload = json.dumps(gearman_payload, default=str)
            gm_client.submit_job("acd-log-processor", bytes(json_payload, 'utf-8'), background=True)
            root_logger.info("Evento EXIT_AMD enviado a Gearman: %s", json_payload)
        except Exception as e:
            root_logger.error("Error enviando evento EXIT_AMD a Gearman: %s", e)
            # No interrumpir el flujo si Gearman falla

        # 5. Leer base y asegurar esquema HTTPS
        base = os.getenv('DIALER_AMD_ENDPOINT', '').strip()
        if not base:
            root_logger.error("DIALER_AMD_ENDPOINT no está definido")
            return

        if not base.startswith(('http://', 'https://')):
            base = 'http://' + base

        # 6. Construir URL final (sin el idContact ahora)
        url = base.rstrip('/') + "/add_amd_event"
        root_logger.debug("POST URL construida: %s", url)

        # 7. Payload con nuevos nombres
        payload = {
            "id_contact": int(idContact),
            "id_campaign": int(idCamp),
            "phone_number": telContact
        }

        # 8. Envío POST (mantener para compatibilidad)
        try:
            resp = requests.post(
                url,
                json=payload,
                timeout=5,
                verify=False
            )
            if not resp.ok:
                root_logger.error(
                    "POST a %s falló: %s – %s",
                    url, resp.status_code, resp.text
                )
                return
            root_logger.info("POST correcto a %s → %s", url, payload)
        except Exception as e:
            root_logger.error("Error haciendo POST a %s: %s", url, e)

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
