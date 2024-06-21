from pyModbusTCP.client import ModbusClient
import requests
import os
import logging.config
import logging.handlers
import time
from conversions import get_shift

# logs dir
if not os.path.isdir("./logs"):
    logging.info("[-] logs directory doesn't exists")
    os.mkdir("./logs")
    logging.info("[+] Created logs dir successfully")
dirname = os.path.dirname(os.path.abspath(__file__))
logging.config.fileConfig('logging.config')
logger = logging.getLogger('JayFee_log')
# end region

GL_MACHINE_INFO = {
    'cutting-1': {
        'py_ok': True,
        'ip': '192.168.0.135',
        'stage': 'HQT-1',
        'line': 'Line 1',
    },
    'cutting-2': {
        'py_ok': True,
        'ip': "192.168.0.152",
        'stage': 'HQT-2',
        'line': 'Line 2',
    }
}

SEND_DATA = True
HOST = 'ithingspro.cloud'
ACCESS_TOKEN = '0MQPNsT7hoE65h1BRtMV'
PREV_SHIFT = 'A'
HEADERS = {"Content-Type": "application/json"}
STATUS = False
SENT_FLAG = False
# DATA GATHERING

def init_conf():
    global GL_MACHINE_NAME, GL_PARAM_LIST, PUBLISH_TOPIC, PY_OK, ENERGY_TOPIC, GL_IP
    global MACHINE_ID, LINE, STAGE
    if not os.path.isdir("./conf"):
        logger.info("[-] conf directory doesn't exists")
        try:
            os.mkdir("./conf")
            logger.info("[+] Created conf dir successfully")
        except Exception as e:
            pass
            logger.error(f"[-] Can't create conf dir Error: {e}")

    try:
        with open('./conf/machine_config.conf', 'r') as f:
            data = f.readline().replace("\n", "")
            data = {data.split('=')[0]: data.split('=')[1]}
            logging.info(data)
            logging.info(type(data))

            GL_MACHINE_NAME = data['m_name']
            PY_OK = GL_MACHINE_INFO[GL_MACHINE_NAME]['py_ok']
            STAGE = GL_MACHINE_INFO[GL_MACHINE_NAME]["stage"]
            LINE = GL_MACHINE_INFO[GL_MACHINE_NAME]["line"]
            GL_IP = GL_MACHINE_INFO[GL_MACHINE_NAME]['ip']
            logging.info(f"[+] Machine_name is {GL_MACHINE_NAME}")
    except FileNotFoundError as e:
        logger.error(f'[-] machine_config.conf not found {e}')
        with open('./conf/machine_config.conf', 'w') as f:
            data = "m_name=NO_MACHINE"
            f.write(data)


logger.info(f"[+] Initialising configuration")
init_conf()
logger.info(f"[+] Machine is {GL_MACHINE_NAME}")
logger.info(f"[+] Machine stage is {STAGE}")
logger.info(f"[+] Machine IP is {GL_IP}")
logger.info(f"[+] Trigger topic is {PY_OK}")


def Connection():
    global GL_IP
    c = ModbusClient(host=GL_IP, port=510, unit_id=1, auto_open=True)
    return c


def Reading_data():
    try:
        c = Connection()
        regs = c.read_input_registers(0, 10)
        c.close()
        time.sleep(2)
        return regs
    except Exception as err:
        logger.error(f'Error is in Reading Data from PLC {err}')


def post_data(payload):
    """posting OEE DATA to the SERVER"""
    url = f'http://{HOST}/api/v1/{ACCESS_TOKEN}/telemetry'
    logging.info("[+] Sending data to server")
    if SEND_DATA:
        try:
            send_req = requests.post(url, json=payload, headers=HEADERS, timeout=2)
            logging.info(send_req.status_code)
            send_req.raise_for_status()
        except Exception as er:
            logging.error(f"[-] Error in sending Cycle time data {er}")


if __name__ == '__main__':
    try:
        while True:
            data = Reading_data()
            logging.info(f'data from plc is {data}')
            shift = get_shift()
            if not SENT_FLAG and data[1]:
                payload = {'status': 'on', 'shift': shift}
                post_data(payload)
                SENT_FLAG = True
                time.sleep(2)
                STATUS = False
            if not data[1] and SENT_FLAG:
                payload = {'status': 'off', 'shift': shift}
                post_data(payload)
                SENT_FLAG = False
            if shift != PREV_SHIFT:
                payload1 = {'shift': shift}
                post_data(payload1)
                PREV_SHIFT = shift

    except KeyboardInterrupt:
        logger.info('Interrupted')
