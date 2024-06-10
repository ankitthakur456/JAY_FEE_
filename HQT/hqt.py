#!/usr/bin/env python
import pika
from pyModbusTCP.client import ModbusClient
import requests
from database import DBHelper
import asyncio
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import minimalmodbus
import serial
import serial.tools.list_ports
import json
import os
import logging.config
import logging.handlers
from datetime import datetime, timedelta
import struct
from conversions import get_shift
from dotenv import load_dotenv

# Load the .env file
load_dotenv()

# logs dir
if not os.path.isdir("./logs"):
    print("[-] logs directory doesn't exists")
    os.mkdir("./logs")
    print("[+] Created logs dir successfully")
dirname = os.path.dirname(os.path.abspath(__file__))
logging.config.fileConfig('logging.config')
logger = logging.getLogger('JayFee_log')
# end region

GL_MACHINE_INFO = {
    'HQT-1': {
        'py_ok': True,
        'ip': '192.168.0.157',
        'stage': 'HQT-1',
        'line': 'Line 1',
    },
    'HQT-2': {
        'py_ok': True,
        'ip': "192.168.0.152",
        'stage': 'HQT-2',
        'line': 'Line 2',
    }
}

HOST = os.getenv('HOST')
PASSWORD = os.getenv('PASSWORD')
PORT = os.getenv('PORT')
USERNAME_ = os.getenv("USERNAME_")

ADD_SERIAL_NUMBER = os.getenv('ADD_SERIAL_NUMBER')
DEL_SERIAL_NUMBER = os.getenv('DEL_SERIAL_NUMBER')
SEND_ACK_ADDING = os.getenv('SEND_ACK_ADDING')
SEND_ACK_DELETE = os.getenv('SEND_ACK_DELETE')
SEND_DATA_QUEUE = os.getenv('SEND_DATA_QUEUE')
SEND_DATA = True
API = "https://ithingspro.cloud/Jay_FE/api/v1/jay_fe/create_hqt_data/"

HEADERS = {"Content-Type": "application/json"}

PREV_FL_STATUS = False
FL_STATUS = False
gl_HARDING_TEMP_1 = []
gl_HARDING_TEMP_2 = []
gl_HARDING_TEMP_3 = []
gl_HARDING_TEMP_4 = []
ob_db = DBHelper()
ihf_temperature = 0
ihf_entering = 0
o2_gas_pressure = 0
png_pressure = 0

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


def get_serial_port():
    try:
        ports = serial.tools.list_ports.comports()
        usb_ports = [p.device for p in ports if "USB" in p.description]
        logger.info(usb_ports)
        if len(usb_ports) < 1:
            raise Exception("Could not find USB ports")
        return usb_ports[0]
    except Exception as e:
        logger.error(f"[-] Error Can't Open Port {e}")
        return None


def initiate_modbus(slaveId):
    com_port = None
    for i in range(5):
        com_port = get_serial_port()
        if com_port:
            break
    i = int(slaveId)
    instrument = minimalmodbus.Instrument(com_port, i)
    instrument.serial.baudrate = 9600
    instrument.serial.bytesize = 8
    instrument.serial.parity = serial.PARITY_NONE
    instrument.serial.stopbits = 1
    instrument.serial.timeout = 3
    instrument.serial.close_after_each_call = True
    logger.info("Modbus ID Initialized: " + str(i))
    return instrument


def get_machine_data():
    try:
        data_list = []
        param_list = ["hardening_temp_control_1", "hardening_temp_control_2", "hardening_temp_control_3",
                      "hardening_temp_control_4", "tempering_temp_control_1", "tempering_temp_control_2",
                      "tempering_temp_control_3", "tempering_temp_control_4", "tempering_temp_control_5",
                      "tempering_temp_control_6", "quenching_tank_temp_control"]

        for slave_id in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]:
            logger.info(f"[+] Getting data for slave id {slave_id}")
            reg_len = 1
            try:
                data = None
                for i in range(2):
                    mb_client = initiate_modbus(slave_id)
                    data = mb_client.read_registers(2000, reg_len, 4)
                    if data:
                        break
                logger.info(f"Got data {data}")
                if data is None:
                    for i in range(reg_len):
                        data_list.append(0)
                else:
                    data_list += data
            except Exception as e:
                logger.error(f"[+] Failed to get data {e} slave id {slave_id}")
                for i in range(reg_len):
                    data_list.append(0)
        logger.info(f"[*] Got data {data_list}")
        payload = {}
        for index, key in enumerate(param_list):
            payload[key] = data_list[index]
        return payload
    except Exception as e:
        logger.error(e)


def reading_status():
    try:
        c = ModbusClient(host='192.168.0.157', port=510, unit_id=1, auto_open=True)
        regs = c.read_discrete_inputs(0, 2)
        return regs
    except Exception as err:
        logger.error(f'Error PLC disconnected {err}')


async def receive_message(queue_name, host=HOST, port=PORT, username=USERNAME_, password=PASSWORD):
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    def callback(ch, method, properties, body):
        logger.info(f" [x] Received {body} ")
        ob_db.enqueue_serial_number(body.decode('utf-8'))
        # Send acknowledgment
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return body

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    logger.info(' [*] Waiting for messages.')
    # Start consuming
    channel.start_consuming()


def thread_target(queue_name):
    asyncio.run(receive_message(queue_name))


async def send_message(body, queue_name, host=HOST, port=PORT, username=USERNAME_, password=PASSWORD):
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(exchange='', routing_key=queue_name, body=body, properties=pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent))
    logger.info(f" [x] Sent '{body}' to '{queue_name}'")
    connection.close()


def post_data(DATA):
    if SEND_DATA:
        try:
            send_req = requests.post(API, json=DATA, headers=HEADERS, timeout=2)
            logging.info(send_req.status_code)
            send_req.raise_for_status()
            data = ob_db.get_sync_data()

            if data:
                try:
                    for value in data:
                        payload = json.loads(value[0])
                        logging.info(f"Payload to send sync {payload}")
                        sync_req = requests.post(API, json=payload, headers=HEADERS, timeout=5)
                        sync_req.raise_for_status()
                        logging.info(f"payload send from sync data : {sync_req.status_code}")

                except Exception as e:
                    logging.error(f"[-] Error in sending SYNC data {e}")
                else:
                    ob_db.delete_sync_data()
            else:
                logging.info(f"Synced data is empty")

        except Exception as e:
            logging.error(f"[-] Error in sending data TO API, {e}")
            ob_db.add_sync_data(DATA)


def main():
    global gl_HARDING_TEMP_1, gl_HARDING_TEMP_2, gl_HARDING_TEMP_3, gl_TEMPERING_TEMP_2, gl_HARDING_TEMP_4, gl_TEMPERING_TEMP_3, gl_TEMPERING_TEMP_4, gl_TEMPERING_TEMP_5, gl_TEMPERING_TEMP_6, gl_QUENCHING_TANK_TEMP_CONTROL, gl_TEMPERING_TEMP_1
    while True:
        ob_db = DBHelper()
        try:
            data = get_machine_data()
            logging.info(f'data rom salave is {data}')
            status = reading_status()
            logger.info(f"status from plc is {status}")
            logger.info("Door is closed. Outside the inner loop.")
            # if status[0]:
            gl_HARDING_TEMP_1 = data.get('hardening_temp_control_1')
            gl_HARDING_TEMP_2 = data.get('hardening_temp_control_2')
            gl_HARDING_TEMP_3 = data.get('hardening_temp_control_3')
            gl_HARDING_TEMP_4 = data.get('hardening_temp_control_4')
            gl_TEMPERING_TEMP_1 = data.get('tempering_temp_control_1')
            gl_TEMPERING_TEMP_2 = data.get('tempering_temp_control_2')
            gl_TEMPERING_TEMP_3 = data.get('tempering_temp_control_3')
            gl_TEMPERING_TEMP_4 = data.get('tempering_temp_control_4')
            gl_TEMPERING_TEMP_5 = data.get('tempering_temp_control_5')
            gl_TEMPERING_TEMP_6 = data.get('tempering_temp_control_6')
            gl_QUENCHING_TANK_TEMP_CONTROL = data.get('quenching_tank_temp_control')
            # Perform other processing here
            logging.info("Processing inside inner loop...")
            serial_number = ob_db.get_first_serial_number()
            logging.info(f'serial number is {serial_number}')
            if serial_number and status[1]:
                shift = get_shift()
                asyncio.run(send_message(serial_number, SEND_ACK_ADDING))
                try:
                    time_ = datetime.now().isoformat()
                    date = (datetime.now() - timedelta(hours=7)).strftime("%F")
                    DATA = {
                        "serial_number": serial_number,
                        "time_": time_,
                        "date_": date,
                        "line": LINE,
                        "machine": GL_MACHINE_NAME,
                        "shift": shift,
                        "py_ok": PY_OK,
                        "hardening_temp_control_1": gl_HARDING_TEMP_1,
                        "hardening_temp_control_2": gl_HARDING_TEMP_2,
                        "hardening_temp_control_3": gl_HARDING_TEMP_3,
                        "hardening_temp_control_4": gl_HARDING_TEMP_4,
                        "tempering_temp_control_1": gl_TEMPERING_TEMP_1,
                        "tempering_temp_control_2": gl_TEMPERING_TEMP_2,
                        "tempering_temp_control_3": gl_TEMPERING_TEMP_3,
                        "tempering_temp_control_4": gl_TEMPERING_TEMP_4,
                        "tempering_temp_control_5": gl_TEMPERING_TEMP_5,
                        "tempering_temp_control_6": gl_TEMPERING_TEMP_6,
                        "quenching_tank_temp_control": gl_QUENCHING_TANK_TEMP_CONTROL
                    }

                    ob_db.save_running_data(serial_number, gl_HARDING_TEMP_1, gl_HARDING_TEMP_2, gl_HARDING_TEMP_3,
                                            gl_HARDING_TEMP_4, gl_TEMPERING_TEMP_1,
                                            gl_TEMPERING_TEMP_2, gl_TEMPERING_TEMP_3, gl_TEMPERING_TEMP_4,
                                            gl_TEMPERING_TEMP_5, gl_TEMPERING_TEMP_6,
                                            gl_QUENCHING_TANK_TEMP_CONTROL)
                    logger.info(f'payload is {DATA}')
                    post_data(DATA)
                    ob_db.delete_serial_number(serial_number)
                except Exception as e:
                    logger.error(f'serial number is empty {e}')
        except Exception as err:
            logger.error(f'error in executing main {err}')


if __name__ == '__main__':
    try:
        Serial_Number_Container = Queue()
        with ThreadPoolExecutor() as executor:
            future = executor.submit(thread_target, ADD_SERIAL_NUMBER)
            future1 = executor.submit(main)
            if future.done():
                logger.info("thread_target task completed")
            else:
                logger.info("thread_target task still running")
            if future1.done():
                logger.info("main task completed")
            else:
                logger.info("main task still running")
                # Sleep for a specified time before the next iteration
    except KeyboardInterrupt:
        logger.error('Interrupted')
