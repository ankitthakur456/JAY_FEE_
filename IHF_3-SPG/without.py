from pyModbusTCP.client import ModbusClient
import time
import requests
import os
import logging.config
import logging.handlers
import struct

# HANDLING LOGGING
if not os.path.isdir("./logs"):
    print("[-] logs directory doesn't exists")
    os.mkdir("./logs")
    print("[+] Created logs dir successfully")
dirname = os.path.dirname(os.path.abspath(__file__))
logging.config.fileConfig('logging.config')
logger = logging.getLogger('JayFee_log')
# END LOGGING


# Global vars
PREV_FL_STATUS = False
FL_STATUS = False
IHF_HEATING = 0
SPG_HEATING = 0
OXYGEN_HEATING = 0
PNG_PRESSURE = 0
AIR_PRESSURE = 0
ihf_heating = []
spg_heating = []
oxygen_heating = []
png_pressure = []
air_pressure = []
DAAcetylenePressure = []
ACCESS_TOKEN = ''
URL_TELE = f'https://ithingspro.cloud/api/v1/{ACCESS_TOKEN}/telemetry'


def Connection():
    c = ModbusClient(host="192.168.0.107", port=510, unit_id=1, auto_open=True)
    return c


def Reading_data():
    try:
        c = Connection()
        regs = c.read_input_registers(0, 12)
        logger.info(f'reading data from  plc {regs}')
        c.close()
        return regs
    except Exception as err:
        logger.error(f'Error is in Reading Data from PLC {err}')


def send_data(data):
    try:
        if data:
            response = requests.post(URL_TELE, json=data, timeout=3)
            response.raise_for_status()
            logger.info(f"Production data sent (status:{response.status_code})")
    except Exception as err:
        logger.error(f"Error: {err}")


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


if __name__ == "__main__":
    while True:
        try:
            data = Reading_data()
            ihf = modbus_to_float_cdab([data[0], data[1]])
            print(f'ihf_heating is {ihf}')
            spg = modbus_to_float_cdab([data[2], data[3]])
            logger.info(f'spg_heating is {spg}')
            oxygen = modbus_to_float_cdab([data[4], data[5]])
            logger.info(f'oxygen_heating is {oxygen}')
            png = modbus_to_float_cdab([data[6], data[7]])
            logger.info(f'png_pressure is {png}')
            air = modbus_to_float_cdab([data[8], data[9]])
            logger.info(f'air_pressure is {air}')
            DA = modbus_to_float_cdab([data[10], data[11]])

            if spg > 100:
                FL_STATUS = True
                logger.info(f'cycle running')
            else:
                FL_STATUS = False
                logger.info(f'cycle stopped')

            if FL_STATUS:
                ihf_heating.append(ihf)
                spg_heating.append(spg)
                oxygen_heating.append(oxygen)
                png_pressure.append(png)
                air_pressure.append(air)
                DAAcetylenePressure.append(DA)

            if FL_STATUS != PREV_FL_STATUS:
                logger.info(f'ihf_heating list is {ihf_heating}')
                logger.info(f'spg_heating list is {spg_heating}')
                logger.info(f'oxygen_heating list is {oxygen_heating}')
                logger.info(f'png_pressure list is {png_pressure}')
                logger.info(f'air_pressure list is {air_pressure}')
                logger.info(f'air_pressure list is {DAAcetylenePressure}')

                payload = {
                    'IHF_HEATING-3': max(ihf_heating),
                    'SPG_HEATING-3': max(spg_heating),
                    'OXYGEN_HEATING-3': max(oxygen_heating),
                    'PNG_PRESSURE-3': max(png_pressure),
                    'AIR_PRESSURE-3': max(air_pressure),
                    'DAAcetylenePressure': max(DAAcetylenePressure)
                }
                if FL_STATUS:
                    logger.info(f'cycle running')
                if not FL_STATUS:
                    logger.info(f'payload is {payload}')
                    send_data(payload)
                    logger.info(f'prev_FL_STATUS is {PREV_FL_STATUS}')
            PREV_FL_STATUS = FL_STATUS
            ihf_heating = []
            spg_heating = []
            oxygen_heating = []
            png_pressure = []
            air_pressure = []
            time.sleep(5)
        except Exception as err:
            logger.error(f'error in executing main function data in not comming from PLC {err}')

