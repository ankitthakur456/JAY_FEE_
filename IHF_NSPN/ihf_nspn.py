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
    'SPG-1': {
        'py_ok': True,
        'ip': '192.168.0.1',
        'stage': 'IHF_BSPN',
        'line': 'Line 2',
    },
    'SPG-2': {
        'py_ok': True,
        'ip': "192.168.0.105",
        'stage': 'IHF_NSPN',
        'line': 'Line 2',
    }
}

HOST = os.getenv('HOST')
PASSWORD = os.getenv('PASSWORD')
PORT = os.getenv('PORT')
USERNAME_ = os.getenv("USERNAME_")

ADD_SERIAL_NUMBER = os.getenv('ADD_SERIAL_NUMBER')
ADD_SERIAL_NUMBER1 = 'priority_add_srl_300_1'  # os.getenv('ADD__PRIORITY_SERIAL_NUMBER')
DEL_SERIAL_NUMBER = os.getenv('DEL_SERIAL_NUMBER')
SEND_ACK_ADDING = os.getenv('SEND_ACK_ADDING')
SEND_ACK_DELETE = os.getenv('SEND_ACK_DELETE')
SEND_DATA_QUEUE = os.getenv('SEND_DATA_QUEUE')
SEND_DATA = True
API = "https://ithingspro.cloud/Jay_FE/api/v1/jay_fe/create_neck_spinning_data/"
HEADERS = {"Content-Type": "application/json"}

PREV_FL_STATUS = False
FL_STATUS = False
gl_IHF_HEATING_LIST = []
gl_SPG_HEATING_LIST = []
gl_OXYGEN_HEATING_LIST = []
gl_PNG_PRESSURE_LIST = []
gl_DA_GAS_PRESSURE_LIST = []
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
            print(data)
            print(type(data))

            GL_MACHINE_NAME = data['m_name']
            PY_OK = GL_MACHINE_INFO[GL_MACHINE_NAME]['py_ok']
            STAGE = GL_MACHINE_INFO[GL_MACHINE_NAME]["stage"]
            LINE = GL_MACHINE_INFO[GL_MACHINE_NAME]["line"]
            GL_IP = GL_MACHINE_INFO[GL_MACHINE_NAME]['ip']
            print(f"[+] Machine_name is {GL_MACHINE_NAME}")
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
    c = ModbusClient(host='192.168.0.105', port=510, unit_id=1, auto_open=True)
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
        c = ModbusClient(host='192.168.0.147', port=510, unit_id=1, auto_open=True)
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

    while True:
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(host, port, '/', credentials)
        connection = pika.SelectConnection(parameters=parameters, on_open_callback=on_open)
        try:
            connection.ioloop.start()
        except KeyboardInterrupt:
            connection.close()
            connection.ioloop.start()


def thread_target(queue_name1, queue_name2):
    while True:
        asyncio.run(receive_message(queue_name1, queue_name2))


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
            print(DATA)
            print(send_req.status_code)
            send_req.raise_for_status()
        except Exception as e:
            print(f"[-] Error in sending data TO API, {e}")
            ob_db.add_sync_data(DATA)


def post_sync_data():
    data = ob_db.get_sync_data()
    if data:
        try:
            for value in data:
                payload = json.loads(value[0])
                print(f"Payload to send sync {payload}")
                sync_req = requests.post(API, json=payload, headers=HEADERS, timeout=5)
                sync_req.raise_for_status()
                print(f"payload send from sync data : {sync_req.status_code}")

        except Exception as e:
            print(f"[-] Error in sending SYNC data {e}")
        else:
            ob_db.delete_sync_data()
    else:
        print(f"Synced data is empty")


def main():
    global FL_STATUS, PREV_FL_STATUS, gl_SPG_HEATING_LIST, gl_IHF_HEATING_LIST, gl_OXYGEN_HEATING_LIST, gl_PNG_PRESSURE_LIST, gl_AIR_PRESSURE_LIST, gl_DA_GAS_PRESSURE_LIST
    global ihf_temperature, ihf_entering, o2_gas_pressure, png_pressure
    while True:
        ob_db = DBHelper()
        try:
            post_sync_data()
            data = Reading_data()
            status = reading_status()
            logger.info(f'status from machine is {status}')
            ihf_heating = round(float_conversion([data[0], data[1]]), 2)
            logger.info(f'ihf_heating_ is {ihf_heating}')
            spg_heating = round(float_conversion([data[2], data[3]]), 2)
            logger.info(f'spg_heating_ is {spg_heating}')
            oxygen_heating = round(float_conversion([data[4], data[5]]), 2)
            logger.info(f'oxygen_heating_ is {oxygen_heating}')
            png_pressure = round(float_conversion([data[6], data[7]]), 2)
            logger.info(f'png_pressure_ is {png_pressure}')
            da_gas_pressure = round(float_conversion([data[8], data[9]]), 2)
            logger.info(f'da_gas_pressure is {da_gas_pressure}')
            if status[1] >= 10:
                FL_STATUS = True
                logger.info(f'cycle running')
            elif status[1] == 0:
                FL_STATUS = False
                logger.info(f'cycle stopped')
            if FL_STATUS:
                gl_IHF_HEATING_LIST.append(ihf_heating)
                gl_SPG_HEATING_LIST.append(spg_heating)
                gl_OXYGEN_HEATING_LIST.append(oxygen_heating)
                gl_PNG_PRESSURE_LIST.append(png_pressure)
                gl_DA_GAS_PRESSURE_LIST.append(da_gas_pressure)

            if FL_STATUS != PREV_FL_STATUS:
                try:
                    if FL_STATUS:
                        logger.info(f'cycle running')
                    if not FL_STATUS:
                        serial_number = ob_db.get_first_serial_number()
                        logging.info(f'serial number is {serial_number}')
                        priority_serial_number = ob_db.get_first_priority_serial()
                        logging.info(f'priority serial number is {priority_serial_number}')
                        if priority_serial_number:
                            serial_n = priority_serial_number
                        else:
                            serial_n = serial_number
                        if serial_n:
                            shift = get_shift()
                            asyncio.run(send_message(serial_number, SEND_ACK_ADDING))
                            logger.info(f'gl_IHF_HEATING_LIST list is {gl_IHF_HEATING_LIST}')
                            logger.info(f'gl_SPG_HEATING_LIST list is {gl_SPG_HEATING_LIST}')
                            logger.info(f'gl_OXYGEN_HEATING_LIST list is {gl_OXYGEN_HEATING_LIST}')
                            logger.info(f'gl_PNG_PRESSURE_LIST     list is {gl_PNG_PRESSURE_LIST}')
                            logger.info(f'gl_DA_GAS_PRESSURE_LIST list is {gl_DA_GAS_PRESSURE_LIST}')
                            ihf_temperature = max(gl_IHF_HEATING_LIST)
                            ihf_entering = max(gl_SPG_HEATING_LIST)
                            spindle_speed = 150
                            spindle_feed = 300
                            o2_gas_pressure = max(gl_OXYGEN_HEATING_LIST)
                            png_pressure = max(gl_PNG_PRESSURE_LIST)
                            da_gas_pressure = max(gl_DA_GAS_PRESSURE_LIST)
                            try:
                                time_ = datetime.now().isoformat()
                                date = (datetime.now() - timedelta(hours=7)).strftime("%F")

                                DATA = {
                                    "serial_number": serial_n,
                                    "time_": time_,
                                    "date_": date,
                                    "line": LINE,
                                    "machine": GL_MACHINE_NAME,
                                    "shift": shift,
                                    "py_ok": PY_OK,
                                    "ihf_temperature": round(ihf_temperature, 2),
                                    "ihf_entering": round(ihf_entering, 2),
                                    "spindle_speed": round(spindle_speed, 2),
                                    "spindle_feed": round(spindle_feed, 2),
                                    "o2_gas_pressure": round(o2_gas_pressure, 2),
                                    "png_pressure": round(png_pressure, 2)
                                }
                                logger.info(f'da_gas_pressure is {da_gas_pressure}')
                                ob_db.save_running_data(serial_number, ihf_temperature, ihf_entering, o2_gas_pressure,
                                                        png_pressure)
                                logger.info(f'payload is {DATA}')
                                post_data(DATA)
                                ob_db.delete_serial_number(serial_n)
                                ob_db.delete_priority_serial(serial_n)
                                gl_IHF_HEATING_LIST = []
                                gl_SPG_HEATING_LIST = []
                                gl_OXYGEN_HEATING_LIST = []
                                gl_PNG_PRESSURE_LIST = []
                                gl_DA_GAS_PRESSURE_LIST = []
                                ihf_temperature = 0
                                ihf_entering = 0
                                o2_gas_pressure = 0
                                png_pressure = 0
                            except Exception as e:
                                logger.error(f'serial number is empty {e}')
                except Exception as e:
                    logger.error(f'serial number is empty {e}')
            PREV_FL_STATUS = FL_STATUS
            time.sleep(2)
        except Exception as err:
            logger.error(f'error in executing main {err}')


def check_threads(futures):
    for future in futures:
        if future.done() or future.exception():
            logger.info("Thread completed or raised an exception")
            # Trigger the receive_message function
            asyncio.run(receive_message(ADD_SERIAL_NUMBER, ADD_SERIAL_NUMBER1))


def main_executor():
    with ThreadPoolExecutor() as executor:
        futures = []
        futures.append(executor.submit(thread_target, ADD_SERIAL_NUMBER, ADD_SERIAL_NUMBER1))
        futures.append(executor.submit(main))

        # Monitor the threads periodically
        while True:
            check_threads(futures)
            time.sleep(2)  # Adjust the interval as needed


if __name__ == '__main__':
    try:
        main_executor()
    except KeyboardInterrupt:
        logger.error('Interrupted')
