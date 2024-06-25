#!/usr/bin/env python
import pika
from pyModbusTCP.client import ModbusClient
import requests
from database import DBHelper
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
import logging.config
import logging.handlers
from datetime import datetime, timedelta
import struct
from conversions import get_shift
from dotenv import load_dotenv
from statistics import mean

# Load the .env file
load_dotenv()

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
    'SPG-3': {
        'py_ok': True,
        'ip': '192.168.0.107',
        'stage': 'IHF_BSPN-2',
        'line': 'Line 1',
    },
    'SPG-4': {
        'py_ok': True,
        'ip': '192.168.0.107',
        'stage': 'IHF_NSPN-2',
        'line': 'Line 1',
    }
}

HOST = os.getenv('HOST')
PASSWORD = os.getenv('PASSWORD')
PORT = os.getenv('PORT')
USERNAME_ = os.getenv("USERNAME_")

# ADD_QUEUE_NAMES = os.getenv('ADD_QUEUE_NAMES')
ADD_QUEUE_NAME = 'add_srl_100_2'
ADD_QUEUE_NAME1 = 'priority_add_srl_100_2'

DEL_SERIAL_NUMBER = os.getenv('DEL_SERIAL_NUMBER')
SEND_ACK_ADDING = os.getenv('SEND_ACK_ADDING')
SEND_ACK_DELETE = os.getenv('SEND_ACK_DELETE')
SEND_DATA_QUEUE = os.getenv('SEND_DATA_QUEUE')
SEND_DATA = True
API = "https://ithingspro.cloud/Jay_FE/api/v1/jay_fe/create_ihf_data/"
HEADERS = {"Content-Type": "application/json"}

PREV_FL_STATUS = False
FL_STATUS = False
gl_IHF_HEATING_LIST = []
gl_SPG_HEATING_LIST = []
gl_OXYGEN_HEATING_LIST = []
gl_PNG_PRESSURE_LIST = []
gl_AIR_PRESSURE_LIST = []
gl_DAACETYLENE_PRESSURE_LIST = []
ob_db = DBHelper()


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
    c = ModbusClient(host=GL_IP, port=510, unit_id=1, auto_open=True)
    return c


def Reading_data():
    try:
        c = Connection()
        regs = c.read_input_registers(0, 12)
        logger.info(f"values from register is {regs}")
        c.close()
        if not regs:
            a = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            return a
        else:
            return regs
    except Exception as err:
        logger.error(f'Error PLC disconnected {err}')


def reading_status():
    try:
        c = ModbusClient(host='192.168.0.154', port=510, unit_id=1, auto_open=True)
        regs = c.read_holding_registers(0, 5)

        return regs
    except Exception as err:
        logger.error(f'Error is in Reading Data from PLC {err}')


def float_conversion(registers):
    if len(registers) != 2:
        raise ValueError("Expected a list of two 16-bit registers")
    # Convert registers to bytes
    byte1 = (registers[0] >> 8) & 0xFF  # High byte of the first register
    byte2 = registers[0] & 0xFF  # Low byte of the first register
    byte3 = (registers[1] >> 8) & 0xFF  # High byte of the second register
    byte4 = registers[1] & 0xFF  # Low byte of the second register
    # Rearrange bytes to CDAB order
    reordered_bytes = bytearray([byte3, byte4, byte1, byte2])
    result = struct.unpack('>f', reordered_bytes)[0]
    return result


async def receive_message(queue_name1, queue_name2, host=HOST, port=PORT, username=USERNAME_, password=PASSWORD):
    def queue1_callback(ch, method, properties, body):
        logging.info(" [x] Received queue 1: %r" % body)
        ob_db.enqueue_serial_number(body.decode('utf-8'))

    def queue2_callback(ch, method, properties, body):
        logging.info(" [x] Received queue 2: %r" % body)
        ob_db.enqueue_priority_serial(body.decode('utf-8'))

    def on_open(connection):
        connection.channel(on_open_callback=on_channel_open)

    def on_channel_open(channel):
        channel.basic_consume(queue_name1, queue1_callback, auto_ack=True)
        channel.basic_consume(queue_name2, queue2_callback, auto_ack=True)

    try:
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(host, port, '/', credentials)
        connection = pika.SelectConnection(parameters=parameters, on_open_callback=on_open)
        connection.ioloop.start()
    except Exception as e:
        logger.error(f'Exception in receive_message: {e}')
        time.sleep(5)  # Retry after 5 seconds


def thread_target(queue_name1, queue_name2):
    while True:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(receive_message(queue_name1, queue_name2))
        time.sleep(0.5)


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
            logging.info(DATA)
            logging.info(send_req.status_code)
            send_req.raise_for_status()
        except Exception as e:
            logging.info(f"[-] Error in sending data TO API, {e}")
            ob_db.add_sync_data(DATA)


def post_sync_data():
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
            logging.info(f"[-] Error in sending SYNC data {e}")
        else:
            ob_db.delete_sync_data()
    else:
        logging.info(f"Synced data is empty")


def main():
    global FL_STATUS, PREV_FL_STATUS, gl_SPG_HEATING_LIST, gl_IHF_HEATING_LIST, gl_OXYGEN_HEATING_LIST, gl_PNG_PRESSURE_LIST, gl_AIR_PRESSURE_LIST, gl_DAACETYLENE_PRESSURE_LIST, Ser_Num
    while True:
        ob_db = DBHelper()
        try:
            data = Reading_data()
            status = reading_status()
            logging.info(f'status from plc is {status}')
            ihf_heating = float_conversion([data[2], data[3]])
            logger.info(f'spg_heating_ is {ihf_heating}')
            spg_heating = float_conversion([data[0], data[1]])
            logger.info(f'ihf_heating_ is {spg_heating}')
            oxygen_heating = float_conversion([data[4], data[5]])
            logger.info(f'oxygen_heating_ is {oxygen_heating}')
            png_pressure = float_conversion([data[6], data[7]])
            logger.info(f'png_pressure_ is {png_pressure}')
            air_pressure = float_conversion([data[8], data[9]])
            logger.info(f'air_pressure_ is {air_pressure}')
            DA_pressure = 4  # float_conversion([data[10], data[11]])
            logger.info(f'DA_pressure_ is {DA_pressure}')
            if spg_heating > 700:
                gl_SPG_HEATING_LIST.append(spg_heating)
            if ihf_heating >= 750:
                FL_STATUS = True
                logger.info(f'cycle running')
            elif ihf_heating <= 750:
                FL_STATUS = False
                logger.info(f'cycle stopped')
            if FL_STATUS:
                gl_IHF_HEATING_LIST.append(ihf_heating)
                gl_OXYGEN_HEATING_LIST.append(oxygen_heating)
                gl_PNG_PRESSURE_LIST.append(png_pressure)
                gl_AIR_PRESSURE_LIST.append(air_pressure)
                gl_DAACETYLENE_PRESSURE_LIST.append(DA_pressure)

            if FL_STATUS != PREV_FL_STATUS:
                try:
                    if FL_STATUS:
                        logger.info(f'cycle running')
                    if not FL_STATUS:
                        priority_serial_number = ob_db.get_first_priority_serial()
                        logging.info(f'priority_serial_number {priority_serial_number}')
                        serial_number = ob_db.get_first_serial_number()
                        # asyncio.run(send_message(serial_number, SEND_ACK_ADDING))
                        logging.info(f'serial number is {serial_number}')
                        shift = get_shift()
                        if priority_serial_number or serial_number:
                            logging.info(f'ihf heating list {gl_IHF_HEATING_LIST}')
                            logger.info(f'spg heating list {gl_SPG_HEATING_LIST}')
                            ihf_entering = round(max(gl_IHF_HEATING_LIST), 2)
                            spg_temp = round(max(gl_SPG_HEATING_LIST), 2)
                            air_pressure = round(max(gl_AIR_PRESSURE_LIST), 2)
                            spindle_speed = 150
                            spindle_feed = 300
                            da_pressure = round(max(gl_DAACETYLENE_PRESSURE_LIST), 2)
                            o2_gas_pressure = round(max(gl_OXYGEN_HEATING_LIST), 2)
                            png_pressure = round(max(gl_PNG_PRESSURE_LIST), 2)
                            time_ = datetime.now().isoformat()
                            date = (datetime.now() - timedelta(hours=7)).strftime("%F")
                            if priority_serial_number:
                                Ser_Num = priority_serial_number
                            else:
                                Ser_Num = serial_number
                            DATA = {
                                "serial_number": Ser_Num,
                                "time_": time_,
                                "date_": date,
                                "line": LINE,
                                "machine": GL_MACHINE_NAME,
                                "shift": shift,
                                "py_ok": PY_OK,
                                "ihf_temperature": spg_temp,
                                "ihf_entering": ihf_entering,
                                "spindle_speed": spindle_speed,
                                "spindle_feed": spindle_feed,
                                "da_gas_pressure": da_pressure,
                                "o2_gas_pressure": o2_gas_pressure,
                                "air_pressure": air_pressure,
                                "png_pressure": png_pressure
                            }

                            ob_db.save_running_data(Ser_Num, spg_temp, ihf_entering, o2_gas_pressure,
                                                    png_pressure,
                                                    air_pressure, da_pressure, spindle_speed, spindle_feed)
                            post_data(DATA)
                            ob_db.delete_serial_number(Ser_Num)
                            ob_db.delete_priority_serial(Ser_Num)
                            gl_IHF_HEATING_LIST = []
                            gl_SPG_HEATING_LIST = []
                            gl_OXYGEN_HEATING_LIST = []
                            gl_PNG_PRESSURE_LIST = []
                            gl_AIR_PRESSURE_LIST = []
                            gl_DAACETYLENE_PRESSURE_LIST = []

                except Exception as e:
                    logger.info(f'error is in {e}')
            PREV_FL_STATUS = FL_STATUS
            time.sleep(0.5)
        except Exception as err:
            logger.error(f'error in executing main {err}')



def check_threads(futures, executor):
    for future in futures:
        if future.done() or future.exception():
            logger.info("Thread completed or raised an exception")
            futures.remove(future)
            futures.append(executor.submit(thread_target, ADD_QUEUE_NAME, ADD_QUEUE_NAME1))


def main_executor():
    with ThreadPoolExecutor() as executor:
        futures = []
        futures.append(executor.submit(thread_target, ADD_QUEUE_NAME, ADD_QUEUE_NAME1))
        futures.append(executor.submit(main))
        while True:
            check_threads(futures, executor)
            time.sleep(5)


if __name__ == '__main__':
    try:
        main_executor()
    except KeyboardInterrupt:
        logger.error('Interrupted')
