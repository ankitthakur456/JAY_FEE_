from pyModbusTCP.client import ModbusClient
import requests
import os
import logging.config
import logging.handlers
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
        'ip': '192.168.0.157',
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

HOST = os.getenv('HOST')
SEND_DATA = True
API = "https://ithingspro.cloud/Jay_FE/api/v1/jay_fe/create_hqt_data/"

HEADERS = {"Content-Type": "application/json"}

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

def post_data(DATA):
    if SEND_DATA:
        try:
            send_req = requests.post(API, json=DATA, headers=HEADERS, timeout=2)
            logging.info(DATA)
            logging.info(send_req.status_code)
            send_req.raise_for_status()
        except Exception as e:
            logging.error(f"[-] Error in sending data TO API, {e}")

