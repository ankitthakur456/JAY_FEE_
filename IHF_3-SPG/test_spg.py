#!/usr/bin/env python
import pika
from pyModbusTCP.client import ModbusClient
import requests
from database import DBHelper
import time
import asyncio
import threading
from queue import Queue
import json
import os
import logging.config
import logging.handlers
from datetime import datetime
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
        'ip': '192.168.0.107',
        'stage': 'IHF_NSPN',
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
API = "https://ithingspro.cloud/Jay_FE/api/v1/jay_fe/create_ihf_data/"  # os.getenv("APT")
HEADERS = {"Content-Type": "application/json"}

PREV_FL_STATUS = False
FL_STATUS = False
gl_ihf_heating_list = []
gl_spg_heating_list = []
gl_oxygen_heating_list = []
gl_png_pressure_list = []
gl_air_pressure_list = []
gl_DAAcetylenePressure_list = []
ob_db = DBHelper()
GL_IP = ''


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
    global GL_IP
    c = ModbusClient(host=GL_IP, port=510, unit_id=1, auto_open=True)
    return c


def Reading_data():
    try:
        c = Connection()
        regs = c.read_input_registers(0, 12)
        c.close()
        return regs
    except Exception as err:
        logger.error(f'Error is in Reading Data from PLC {err}')


def modbus_to_float_cdab(registers):
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


async def receive_message(queue_name, SERIAL_NUMBER_CONTAINER, host=HOST, port=PORT, username=USERNAME_,
                          password=PASSWORD):
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    def callback(ch, method, properties, body):
        logger.info(f" [x] Received {body} ")
        SERIAL_NUMBER_CONTAINER.put(body)
        # Send acknowledgment
        ch.basic_ack(delivery_tag=method.delivery_tag)
        # Stop consuming after receiving one message
        # ch.stop_consuming()
        return body

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    logger.info(' [*] Waiting for messages.')
    # Start consuming
    channel.start_consuming()


def thread_target(queue_name, SERIAL_NUMBER_CONTAINER):
    asyncio.run(receive_message(queue_name, SERIAL_NUMBER_CONTAINER))


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

        except Exception as e:
            print(f"[-] Error in sending data TO API, {e}")
            ob_db.add_sync_data(DATA)


def main(SERIAL_NUMBER_CONTAINER):
    global FL_STATUS, PREV_FL_STATUS, gl_spg_heating_list, gl_ihf_heating_list, gl_oxygen_heating_list, gl_png_pressure_list, gl_air_pressure_list, gl_DAAcetylenePressure_list, gl_prev_count
    while True:
        serial_ = None
        ob_db = DBHelper()
        if not SERIAL_NUMBER_CONTAINER.empty():
            serial_ = SERIAL_NUMBER_CONTAINER.get(serial_)
            ob_db.enqueue_serial_number(serial_.decode('utf-8'))
            logger.info(f'serial number is {serial_}')
            if serial_:
                logger.info(f'serial number is {serial_}')
                body = f'serial number is {serial_}'
                asyncio.run(send_message(body, SEND_ACK_ADDING))
        try:
            data = Reading_data()
            ihf_heating = modbus_to_float_cdab([data[0], data[1]])
            logger.info(f'ihf_heating is {ihf_heating}')
            spg_heating = modbus_to_float_cdab([data[2], data[3]])
            logger.info(f'spg_heating is {spg_heating}')
            oxygen_heating = modbus_to_float_cdab([data[4], data[5]])
            logger.info(f'oxygen_heating is {oxygen_heating}')
            png_pressure = modbus_to_float_cdab([data[6], data[7]])
            logger.info(f'png_pressure is {png_pressure}')
            air_pressure = modbus_to_float_cdab([data[8], data[9]])
            logger.info(f'air_pressure is {air_pressure}')
            DA_pressure = modbus_to_float_cdab([data[10], data[11]])
            logger.info(f'DA_pressure is {DA_pressure}')
            time.sleep(2)
            if spg_heating > 100:
                FL_STATUS = True
                logger.info(f'[+] cycle running')
            else:
                FL_STATUS = False
                logger.info(f'[-] cycle stopped')

            if FL_STATUS:
                gl_ihf_heating_list.append(ihf_heating)
                gl_spg_heating_list.append(spg_heating)
                gl_oxygen_heating_list.append(oxygen_heating)
                gl_png_pressure_list.append(png_pressure)
                gl_air_pressure_list.append(air_pressure)
                gl_DAAcetylenePressure_list.append(DA_pressure)

            if FL_STATUS != PREV_FL_STATUS:
                serial_number = ob_db.get_first_serial_number()
                shift = get_shift()
                if FL_STATUS:
                    logger.info(f'[+] cycle running')
                try:
                    if not FL_STATUS and serial_number:
                        logger.info(f'gl_ihf_heating_list list is {gl_ihf_heating_list}')
                        logger.info(f'gl_spg_heating_list list is {gl_spg_heating_list}')
                        logger.info(f'gl_oxygen_heating_list list is {gl_oxygen_heating_list}')
                        logger.info(f'gl_png_pressure_list list is {gl_png_pressure_list}')
                        logger.info(f'gl_air_pressure_list list is {gl_air_pressure_list}')
                        logger.info(f'gl_DAAcetylenePressure_list list is {gl_DAAcetylenePressure_list}')
                        ihf_temperature = max(gl_ihf_heating_list)
                        ihf_entering = max(gl_spg_heating_list)
                        spindle_speed = max(gl_air_pressure_list)
                        spindle_feed = max(gl_DAAcetylenePressure_list)
                        o2_gas_pressure = max(gl_oxygen_heating_list)
                        png_pressure = max(gl_png_pressure_list)
                        time_ = datetime.now().isoformat()
                        date = datetime.now().strftime("%Y-%m-%d")
                        DATA = {
                            "serial_number": serial_number,
                            "time_": time_,
                            "date_": date,
                            "line": LINE,
                            "machine": GL_MACHINE_NAME,
                            "shift": shift,
                            "py_ok": True,
                            "ihf_temperature": round(ihf_temperature, 1),
                            "ihf_entering": round(ihf_entering, 1),
                            "spindle_speed": round(spindle_speed, 1),
                            "spindle_feed": round(spindle_feed, 1),
                            "o2_gas_pressure": round(o2_gas_pressure, 1),
                            "png_pressure": round(png_pressure, 1)
                        }
                        ob_db.save_running_data(serial_number, ihf_temperature, ihf_entering, o2_gas_pressure,
                                                png_pressure,
                                                spindle_speed, spindle_feed)
                        post_data(DATA)
                        ob_db.delete_serial_number(serial_number)
                        gl_ihf_heating_list = []
                        gl_spg_heating_list = []
                        gl_oxygen_heating_list = []
                        gl_png_pressure_list = []
                        gl_air_pressure_list = []
                        gl_DAAcetylenePressure_list = []
                        ihf_heating = 0
                        spg_heating = 0
                        oxygen_heating = 0
                        png_pressure = 0
                        air_pressure = 0
                        DA_pressure = 0
                except Exception as err:
                    logger.error(f'serial number is not avaliable {err}')
                time.sleep(2)
            PREV_FL_STATUS = FL_STATUS
        except Exception as err:
            logger.error(f'error in executing main {err}')
            time.sleep(3)


if __name__ == '__main__':
    try:
        SERIAL_NUMBER_CONTAINER = Queue()
        while True:
            t1 = threading.Thread(target=thread_target, args=(ADD_SERIAL_NUMBER, SERIAL_NUMBER_CONTAINER))
            t2 = threading.Thread(target=main, args=(SERIAL_NUMBER_CONTAINER,))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            time.sleep(3)
    except KeyboardInterrupt:
        logger.info('Interrupted')
