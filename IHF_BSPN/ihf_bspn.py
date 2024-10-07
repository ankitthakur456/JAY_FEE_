#!/usr/bin/env python
import pika
from pyModbusTCP.client import ModbusClient
import requests
from database import DBHelper
import time
import asyncio
import threading
from typing import List
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
    logging.info("[-] logs directory doesn't exists")
    os.mkdir("./logs")
    logging.info("[+] Created logs dir successfully")
dirname = os.path.dirname(os.path.abspath(__file__))
logging.config.fileConfig('logging.config')
logger = logging.getLogger('JayFee_log')
# end region

GL_MACHINE_INFO = {
    'BS-1': {
        'py_ok': True,
        'ip': '192.168.0.105',
        'stage': 'IHF_BSPN',
        'line': 'Line 1',
    },
    'BS': {
        'py_ok': True,
        'ip': "192.168.0.105",
        'stage': 'IHF_BSPN',
        'line': 'Line 1',
    }
}
machine_name = 'BS-1'
HOST = os.getenv('HOST')
PASSWORD = os.getenv('PASSWORD')
PORT = os.getenv('PORT')
USERNAME_ = os.getenv("USERNAME_")

# ADD_QUEUE_NAMES = os.getenv('ADD_QUEUE_NAMES')
ADD_SERIAL_NUMBER = 'add_srl_100_1'
ADD_SERIAL_NUMBER1 = 'priority_add_srl_100_1'
PREV_SHIFT = None
PREV_VALUE = False
DEL_SERIAL_NUMBER = os.getenv('DEL_SERIAL_NUMBER')
SEND_ACK_ADDING = os.getenv('SEND_ACK_ADDING')
SEND_ACK_DELETE = os.getenv('SEND_ACK_DELETE')
SEND_DATA_QUEUE = os.getenv('SEND_DATA_QUEUE')
SEND_DATA = True
previous_stop_time = None
API = "https://ithingspro.cloud/Jay_FE/api/v1/jay_fe/create_ihf_data/"
BREAK_API = "https://ithingspro.cloud/Jay_FE/api/v1/jay_fe/create_update_breakdown/"
HEADERS = {"Content-Type": "application/json"}

PREV_FL_STATUS = False
FL_STATUS = False
gl_IHF_ENTERING_LIST = []
gl_IHF_HEATING_LIST = []
gl_OXYGEN_HEATING_LIST = []
gl_PNG_PRESSURE_LIST = []
gl_AIR_PRESSURE_LIST = []
gl_DAACETYLENE_PRESSURE_LIST = []
# Initialize a global list to store all payloads
ALL_PAYLOADS = []
PREV_BREAK_STATUS = False
BREAK_STOP_ADDED = False
BREAK_STATUS = False
PREV_VALUE = False
TIME_STATUS = False
BREAK_CHECK = False
PREV_BREAK_CHECK = False
SHIFT_STATUS = False
transition_time = time.time()
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
        regs = c.read_input_registers(0, 13)
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


def float_conversion(registers: List[int]) -> float:
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
            ALL_PAYLOADS.append(new_payload)

            # Send cumulative payloads
            for i in range(1, len(ALL_PAYLOADS) + 1):
                cumulative_payload = ALL_PAYLOADS[:i]
                post_breakdown_data(cumulative_payload)

        # Update the flag to the current stop_time
        previous_stop_time = stop_time
    else:
        # Reset the flag if stop_time is None
        previous_stop_time = None


def main():
    global FL_STATUS, PREV_FL_STATUS, gl_IHF_HEATING_LIST, gl_IHF_ENTERING_LIST, gl_OXYGEN_HEATING_LIST, \
        gl_PNG_PRESSURE_LIST, gl_AIR_PRESSURE_LIST, gl_DAACETYLENE_PRESSURE_LIST, Ser_Num, PREV_VALUE, \
        PREV_BREAK_STATUS, BREAK_STATUS, transition_time, BREAK_CHECK, PREV_BREAK_CHECK, PREV_SHIFT, SHIFT_STATUS, \
        BREAK_STOP_ADDED
    while True:
        ob_db = DBHelper()
        try:
            SHIFT_STATUS = False
            data = Reading_data()
            status = reading_status()
            post_sync_data()
            shift = get_shift()
            planned_break_status = break_check(shift)
            logger.info(f'Breack check is {planned_break_status}')
            date = (datetime.now() - timedelta(hours=7)).strftime("%F")
            ob_db.addBreakDuration(machine_name)
            logger.info(f'status from plc is {status}')
            ihf_entering = float_conversion([data[0], data[1]])
            logger.info(f'ihf_entering_ is {ihf_entering}')
            ihf_heating = float_conversion([data[2], data[3]])
            logger.info(f'ihf_heating_ is {ihf_heating}')
            oxygen_heating = float_conversion([data[4], data[5]])
            logger.info(f'oxygen_heating_ is {oxygen_heating}')
            png_pressure = float_conversion([data[6], data[7]])
            logger.info(f'png_pressure_ is {png_pressure}')
            air_pressure = float_conversion([data[8], data[9]])
            logger.info(f'air_pressure_ is {air_pressure}')
            DA_pressure = 1.5  # float_conversion([data[11], data[12]])
            logger.info(f'DA_pressure_ is {DA_pressure}')
            if ihf_entering > 650:
                gl_IHF_ENTERING_LIST.append(round(ihf_entering, 2))
            # gl_IHF_ENTERING_LIST = [900, 950, 980, 960, 1000, 980]
            if 0 < oxygen_heating < 7:
                gl_OXYGEN_HEATING_LIST.append(round(oxygen_heating, 2))
            if ihf_heating > 700:
                FL_STATUS = True
                BREAK_CHECK = False
                PREV_BREAK_CHECK = False
                logger.info(f'cycle running')
                logger.info(f'[++++++start time++++++++]{datetime.now().time()}')
            elif ihf_heating < 700:
                FL_STATUS = False
                BREAK_CHECK = True
                logger.info(f'cycle stopped')
            if FL_STATUS:
                if ihf_heating < 1000:
                    gl_IHF_HEATING_LIST.append(round(ihf_heating, 2))
                # if ihf_entering < 900:
                gl_PNG_PRESSURE_LIST.append(round(png_pressure, 2))
                gl_AIR_PRESSURE_LIST.append(round(air_pressure, 2))
                gl_DAACETYLENE_PRESSURE_LIST.append(round(abs(DA_pressure), 2))
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
                        priority_serial_number = ob_db.get_first_priority_serial()
                        logger.info(f'priority_serial_number {priority_serial_number}')
                        serial_number = ob_db.get_first_serial_number()
                        # asyncio.run(send_message(serial_number, SEND_ACK_ADDING))
                        logger.info(f'serial number is {serial_number}')

                        if priority_serial_number or serial_number:
                            logger.info(f'ihf heating list {gl_IHF_ENTERING_LIST}')
                            logger.info(f'spg heating list {gl_IHF_HEATING_LIST}')
                            logger.info(f'gl_OXYGEN_HEATING_LIST {gl_OXYGEN_HEATING_LIST} ')
                            logger.info(f' gl_PNG_PRESSURE_LIST list {gl_PNG_PRESSURE_LIST}')
                            ihf_entering = round(max(gl_IHF_ENTERING_LIST), 2)
                            spg_temp = round(max(gl_IHF_HEATING_LIST), 2)
                            air_pressure = round(max(gl_AIR_PRESSURE_LIST), 2)
                            spindle_speed = 150
                            spindle_feed = 300
                            da_pressure = 1.5  # round(max(gl_DAACETYLENE_PRESSURE_LIST), 2)
                            o2_gas_pressure = sum(gl_OXYGEN_HEATING_LIST) // len(
                                gl_OXYGEN_HEATING_LIST)  # round(min(gl_OXYGEN_HEATING_LIST), 2)
                            png_pressure = round(min(gl_PNG_PRESSURE_LIST), 1)
                            time_ = datetime.now().isoformat()

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
                                "ihf_temperature": ihf_entering,
                                "ihf_entering": spg_temp,
                                "spindle_speed": spindle_speed,
                                "spindle_feed": spindle_feed,
                                "da_gas_pressure": da_pressure,
                                "o2_gas_pressure": o2_gas_pressure,
                                "air_pressure": air_pressure,
                                "png_pressure": png_pressure
                            }
                            ob_db.save_running_data(Ser_Num, ihf_entering, spg_temp, o2_gas_pressure,
                                                    png_pressure,
                                                    air_pressure, da_pressure, spindle_speed, spindle_feed)
                            post_data(DATA)
                            ob_db.delete_serial_number(Ser_Num)
                            ob_db.delete_priority_serial(Ser_Num)
                            gl_IHF_ENTERING_LIST = []
                            gl_IHF_HEATING_LIST = []
                            gl_OXYGEN_HEATING_LIST = []
                            gl_PNG_PRESSURE_LIST = []
                            gl_AIR_PRESSURE_LIST = []
                            gl_DAACETYLENE_PRESSURE_LIST = []

                except Exception as e:
                    logger.info(f'error is in {e}')
            PREV_FL_STATUS = FL_STATUS
            time.sleep(0.1)
        except Exception as err:
            logger.error(f'error in executing main {err}')


def check_threads(threads):
    for thread in threads:
        if not thread.is_alive():
            logger.info("Thread completed or raised an exception, restarting...")
            threads.remove(thread)
            new_thread = threading.Thread(target=thread._target)
            threads.append(new_thread)
            new_thread.start()


# Main function to start threads
def main_executor():
    threads = []
    t1 = threading.Thread(target=thread_target)
    t2 = threading.Thread(target=main)
    threads.append(t1)
    threads.append(t2)

    # Start initial threads
    t1.start()
    t2.start()

    while True:
        check_threads(threads)
        time.sleep(0.1)


if __name__ == "__main__":
    main_executor()
