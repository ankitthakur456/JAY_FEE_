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
from conversions import get_shift, break_check
from dotenv import load_dotenv
import socket

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
    'NS-1': {
        'py_ok': True,
        'plc_ip': '192.168.0.156',
        'machine_ip': '192.168.0.103',
        'gateway_ip': '192.168.0.146',
        'stage': 'IHF_BSPN',
        'line': 'Line 1',
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
BREAK_API = "https://ithingspro.cloud/Jay_FE/api/v1/jay_fe/create_update_breakdown/"
HEADERS = {"Content-Type": "application/json"}

PREV_FL_STATUS = False
FL_STATUS = False
machine_name = 'NS-1'
GL_IHF_HEATING_LIST = []
GL_SPG_HEATING_LIST = []
GL_OXYGEN_HEATING_LIST = []
GL_PNG_PRESSURE_LIST = []
GL_DA_GAS_PRESSURE_LIST = []
ob_db = DBHelper()
ihf_temperature = 0
ihf_entering = 0
o2_gas_pressure = 0
png_pressure = 0
ALL_PAYLOADS = []
PREV_SHIFT = None
PREV_VALUE = False
GL_IHF_ENTERING_LIST = []
GL_AIR_PRESSURE_LIST = []
PREV_BREAK_STATUS = False
BREAK_STOP_ADDED = False
BREAK_STATUS = False
PREV_VALUE = False
TIME_STATUS = False
BREAK_CHECK = False
PREV_BREAK_CHECK = False
SHIFT_STATUS = False
transition_time = time.time()
previous_stop_time = None


# DATA GATHERING

def init_conf():
    global GL_MACHINE_NAME, GL_PARAM_LIST, PUBLISH_TOPIC, PY_OK, ENERGY_TOPIC, GL_PLC_IP
    global MACHINE_ID, LINE, STAGE, GL_MACHINE_IP
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
            GL_PLC_IP = GL_MACHINE_INFO[GL_MACHINE_NAME]['plc_ip']
            GL_MACHINE_IP = GL_MACHINE_INFO[GL_MACHINE_NAME]['machine_ip']

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
logger.info(f"[+] PLC IP is {GL_PLC_IP}")
logger.info(f"[+] Machine IP is {GL_MACHINE_IP}")
logger.info(f"[+] Trigger topic is {PY_OK}")


def Connection():
    c = ModbusClient(host=GL_MACHINE_IP, port=510, unit_id=1, auto_open=True)
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
        c = ModbusClient(host=GL_PLC_IP, port=510, unit_id=1, auto_open=True)
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


def is_connected():
    """ Check if the internet connection is available. """
    try:
        # Try to connect to a common DNS server (Google's public DNS)
        socket.create_connection(("8.8.8.8", 53), timeout=5)
        return True
    except OSError:
        return False


async def receive_message(host=HOST, port=PORT, username=USERNAME_, password=PASSWORD):
    connection = None
    while True:
        if is_connected():
            try:
                logger.info("Internet connection is available. Attempting to connect...")
                credentials = pika.PlainCredentials(username, password)
                parameters = pika.ConnectionParameters(host, port, '/', credentials)

                connection = pika.SelectConnection(parameters=parameters, on_open_callback=on_open,
                                                   on_close_callback=on_close)
                connection.ioloop.start()

            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f'AMQP Connection Error: {e}')
                await asyncio.sleep(5)  # Wait before retrying
            except Exception as e:
                logger.error(f'General Exception in receive_message: {e}')
                await asyncio.sleep(5)  # Wait before retrying
            finally:
                if connection and connection.is_open:
                    connection.close()
        else:
            logger.error("Internet connection is down. Waiting for reconnection...")
            while not is_connected():
                await asyncio.sleep(5)  # Check every 5 seconds for reconnection

        logger.info("Reconnecting after a brief pause...")
        await asyncio.sleep(5)  # Optional delay to prevent too frequent retries


def on_open(connection):
    logger.info("Connection opened.")
    connection.channel(on_open_callback=on_channel_open)


def on_channel_open(channel):
    logger.info("Channel opened.")
    channel.basic_consume(ADD_SERIAL_NUMBER, queue1_callback, auto_ack=True)
    channel.basic_consume(ADD_SERIAL_NUMBER1, queue2_callback, auto_ack=True)


def on_close(connection, reason):
    logger.info(f"Connection closed: {reason}")
    connection.ioloop.stop()


def queue1_callback(ch, method, properties, body):
    logger.info(f" [x] Received queue 1: {body}")
    if body:
        ob_db.enqueue_serial_number(body.decode('utf-8'))


def queue2_callback(ch, method, properties, body):
    logger.info(f" [x] Received queue 2: {body}")
    if body:
        ob_db.enqueue_priority_serial(body.decode('utf-8'))


async def receive():
    try:
        while True:
            await receive_message()
            await asyncio.sleep(0.2)
    except KeyboardInterrupt:
        logger.error('Interrupted')
    except Exception as e:
        logger.error(f'Exception in main: {e}')


def thread_target():
    asyncio.run(receive())


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
        data_list = [json.loads(item[0]) for item in data]

        # Format each payload
        def format_payload(payload):
            if isinstance(payload['serial_number'], str):
                payload['serial_number'] = str(payload['serial_number'])
            if isinstance(payload['time_'], str):
                payload['time_'] = str(payload['time_'])
            if isinstance(payload['date_'], str):
                payload['date_'] = str(payload['date_'])
            return payload

        formatted_data_list = [format_payload(data) for data in data_list]
        for payload in formatted_data_list:
            response = requests.post(API, json=payload, headers=HEADERS, timeout=5)

            if response.status_code == 200:
                logger.info(f"Data sent successfully: {payload}")
                ob_db.delete_sync_data()
            else:
                logger.info(f"Error {response.status_code}: {response.text}")

    else:
        logger.info(f"Synced data is empty")


def post_breakdown_data(payload):
    if SEND_DATA:
        try:
            send_req = requests.post(BREAK_API, json=payload, headers=HEADERS, timeout=2)
            logging.info(payload)
            logging.info(send_req.status_code)
            send_req.raise_for_status()
        except Exception as e:
            logging.info(f"[-] Error in sending  breakdown data to server, {e}")


class LimitedList:
    def __init__(self, max_size):
        self.max_size = max_size
        self.data = []

    def append(self, value):
        if len(self.data) >= self.max_size:
            self.data.pop(0)
        self.data.append(value)

    def __repr__(self):
        return repr(self.data)





def breakdowndata(date, prevS, shift):
    global previous_stop_time, ALL_PAYLOADS  # Ensure this refers to the flag and list outside the function

    break_id = ob_db.getBreakId(GL_MACHINE_NAME)
    duration = ob_db.getDurations(GL_MACHINE_NAME)
    start_time = ob_db.getBreakStartTime(GL_MACHINE_NAME)
    stop_time = ob_db.getBreakStopTime(GL_MACHINE_NAME)

    # Only proceed if stop_time is not None
    if stop_time is not None:
        # Check if the stop_time was previously None and is now not None
        if previous_stop_time is None:
            new_payload = {
                "bd_id": break_id[0],
                "date_": str(date),
                "shift": str(shift),
                "machine": GL_MACHINE_NAME,
                "stage": 'IHF_BSPN-1',
                "line": str(LINE),
                "start_time": str(start_time),
                "stop_time": str(stop_time),
                "duration": duration,
                "type": "string",
                "reason": "string",
                "category": "string"
            }

            # Append the new payload to the list of all payloads
            #ALL_PAYLOADS.append(new_payload)
            # Create a limited list with a maximum size of 5
            ALL_PAYLOADS = LimitedList(5)
            ALL_PAYLOADS.append(new_payload)
            # # Send cumulative payloads
            # for i in range(1, len(ALL_PAYLOADS) + 1):
            #     cumulative_payload = ALL_PAYLOADS[:i]
            post_breakdown_data(ALL_PAYLOADS)
            logger.info(f'all breakdown payload is {ALL_PAYLOADS}')
        # Update the flag to the current stop_time
        previous_stop_time = stop_time
    else:
        # Reset the flag if stop_time is None
        previous_stop_time = None


def main():
    global FL_STATUS, PREV_FL_STATUS, GL_IHF_HEATING_LIST, GL_IHF_ENTERING_LIST, GL_OXYGEN_HEATING_LIST, \
        GL_PNG_PRESSURE_LIST, GL_AIR_PRESSURE_LIST, GL_DAACETYLENE_PRESSURE_LIST, Ser_Num, PREV_VALUE, \
        PREV_BREAK_STATUS, BREAK_STATUS, transition_time, BREAK_CHECK, PREV_BREAK_CHECK, PREV_SHIFT, SHIFT_STATUS, \
        BREAK_STOP_ADDED, GL_DA_GAS_PRESSURE_LIST, GL_SPG_HEATING_LIST
    while True:
        ob_db = DBHelper()
        try:
            SHIFT_STATUS = False
            post_sync_data()
            data = Reading_data()
            shift = get_shift()
            date = (datetime.now() - timedelta(hours=7)).strftime("%F")
            planned_break_status = break_check(shift)
            status = reading_status()
            ob_db.addBreakDuration(machine_name)
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

            if ihf_heating > 750:
                GL_IHF_HEATING_LIST.append(ihf_heating)
            if spg_heating > 750:
                FL_STATUS = True
                BREAK_CHECK = False
                PREV_BREAK_CHECK = False
                logger.info(f'cycle running')
            elif spg_heating < 750:
                FL_STATUS = False
                BREAK_CHECK = True
                logger.info(f'cycle stopped')
            if FL_STATUS:
                if spg_heating < 1000:
                    GL_SPG_HEATING_LIST.append(spg_heating)
                if 7 > oxygen_heating:
                    GL_OXYGEN_HEATING_LIST.append(oxygen_heating)
                else:
                    GL_OXYGEN_HEATING_LIST.append(0)
                GL_PNG_PRESSURE_LIST.append(png_pressure)
                GL_DA_GAS_PRESSURE_LIST.append(da_gas_pressure)
            current_value = FL_STATUS
            # Check for transition from 0 to 1
            if PREV_VALUE == True and current_value == False:
                transition_time = time.time()  # Calculate elapsed time
            PREV_VALUE = current_value
            logger.info(f'(time.time() - transition_time) > 270 {(time.time() - transition_time)}')
            logger.info(f'[%] previous break check is {PREV_BREAK_CHECK}')
            logger.info(f'[%] Break Check is {BREAK_CHECK}')
            if shift != PREV_SHIFT:
                breakdowndata(date, PREV_SHIFT, shift)
                SHIFT_STATUS = True
                PREV_SHIFT = shift
            logging.info(f'shift is {shift} and prev_shift is {PREV_SHIFT}')
            if PREV_BREAK_CHECK != BREAK_CHECK and not FL_STATUS and (time.time() - transition_time) > 270:
                logging.info(f'planned break status is {planned_break_status}')

                if not planned_break_status or SHIFT_STATUS:
                    ob_db.addBreakStop(machine_name)
                    breakdowndata(date, PREV_SHIFT, shift)
                    ob_db.addBreakStart(machine_name, date, shift, True)
                    BREAK_STOP_ADDED = False
                    logging.info('[+] Breakstart data added to addBreakStart')
                    BREAK_STATUS = True
                    PREV_BREAK_CHECK = BREAK_CHECK
            if not BREAK_STOP_ADDED:
                if planned_break_status or FL_STATUS:
                    BREAK_CHECK = False
                    PREV_BREAK_CHECK = False
                    ob_db.addBreakStop(machine_name)
                    breakdowndata(date, PREV_SHIFT, shift)
                    BREAK_STOP_ADDED = True
            if FL_STATUS and BREAK_STATUS:
                ob_db.addBreakStop(machine_name)
                BREAK_STATUS = False
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
                            logger.info(f'GL_IHF_HEATING_LIST list is {GL_IHF_HEATING_LIST}')
                            logger.info(f'GL_SPG_HEATING_LIST list is {GL_SPG_HEATING_LIST}')
                            logger.info(f'GL_OXYGEN_HEATING_LIST list is {GL_OXYGEN_HEATING_LIST}')
                            logger.info(f'GL_PNG_PRESSURE_LIST     list is {GL_PNG_PRESSURE_LIST}')
                            logger.info(f'GL_DA_GAS_PRESSURE_LIST list is {GL_DA_GAS_PRESSURE_LIST}')
                            ihf_temperature = max(GL_IHF_HEATING_LIST)
                            ihf_entering = max(GL_SPG_HEATING_LIST)
                            if not GL_SPG_HEATING_LIST:
                                ihf_entering = 0
                            spindle_speed = 150
                            spindle_feed = 300
                            o2_gas_pressure = max(
                                GL_OXYGEN_HEATING_LIST)  # sum(GL_OXYGEN_HEATING_LIST) // len(GL_OXYGEN_HEATING_LIST)
                            png_pressure = min(GL_PNG_PRESSURE_LIST)
                            da_gas_pressure = max(GL_DA_GAS_PRESSURE_LIST)
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
                                    "o2_gas_pressure": int(o2_gas_pressure),
                                    "png_pressure": round(png_pressure, 2)
                                }
                                air_pressure = 1
                                daacetylene_pressure = 1

                                logger.info(f'da_gas_pressure is {da_gas_pressure}')

                                ob_db.save_running_data(serial_number, ihf_temperature, ihf_entering, o2_gas_pressure,
                                                        png_pressure)
                                logger.info(f'payload is {DATA}')
                                post_data(DATA)
                                ob_db.delete_serial_number(serial_n)
                                ob_db.delete_priority_serial(serial_n)
                                GL_IHF_HEATING_LIST = []
                                GL_SPG_HEATING_LIST = []
                                GL_OXYGEN_HEATING_LIST = []
                                GL_PNG_PRESSURE_LIST = []
                                GL_DA_GAS_PRESSURE_LIST = []
                                ihf_temperature = 0
                                ihf_entering = 0
                                o2_gas_pressure = 0
                                png_pressure = 0
                            except Exception as e:
                                logger.error(f'serial number is empty {e}')
                except Exception as e:
                    logger.error(f'serial number is empty {e}')
            PREV_FL_STATUS = FL_STATUS
            time.sleep(0.5)
        except Exception as err:
            logger.error(f'error in executing main {err}')


def check_threads(futures, executor):
    for future in futures:
        if future.done() or future.exception():
            logger.info("Thread completed or raised an exception")
            futures.remove(future)
            futures.append(executor.submit(thread_target))
            futures.append(executor.submit(main))


def main_executor():
    with ThreadPoolExecutor() as executor:
        futures = []
        futures.append(executor.submit(thread_target))
        futures.append(executor.submit(main))
        logger.info(f'tast to submit is {futures}')
        while True:
            check_threads(futures, executor)
            time.sleep(5)


if __name__ == '__main__':
    try:
        main_executor()
    except KeyboardInterrupt:
        logger.error('Interrupted')
