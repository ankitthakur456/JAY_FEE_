import sqlite3
import time
import logging
import ast
import json

logger = logging.getLogger('JayFee_log')


class DBHelper:
    def __init__(self):
        self.connection = sqlite3.connect("JAYFEE.db", check_same_thread=False)
        self.cursor = self.connection.cursor()
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_data_table(ts INTEGER, payload STRING)""")  # sync_data_table
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS queue(id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL, serial_number STRING)""")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS
                                sync_data(
                                    serial_number VARCHAR(2),
                                    date_ STRING,
                                    shift VARCHAR(2),
                                    payload STRING)
                                """)
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS running_data(
                            timestamp INTEGER,
                            IHF_HEATING REAL,
                            SPG_HEATING REAL,
                            OXYGEN_HEATING REAL,
                            PNG_PRESSURE REAL,
                            AIR_PRESSURE REAL,
                            DAAcetylenePressure REAL,
                            serial_number TEXT)""")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS StoredValues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value REAL
            )""")

    # region queue functions
    def enqueue_serial_number(self, serial_number):
        try:
            self.cursor.execute("""SELECT serial_number FROM queue where serial_number = ?""", (serial_number,))
            if self.cursor.fetchone() is None:
                self.cursor.execute("""INSERT INTO queue(serial_number, timestamp) VALUES(?,?)""",
                                    (serial_number, time.time()))
                self.connection.commit()
                logger.info(f"[+] Successful, Serial Number Enqueued to the database")
            else:
                logger.info(f"[-] Failed, Serial Number Already Enqueued to the database")
        except Exception as e:
            logger.error(f"[-] Failed to enqueue serial number Error {e}")

    def get_first_serial_number(self):
        try:
            self.cursor.execute("""SELECT serial_number FROM queue ORDER BY timestamp ASC LIMIT 1""")
            serial_number = self.cursor.fetchone()[0]
            if serial_number:
                return serial_number
            else:
                return None
        except Exception as e:
            logger.error(f"[-] Failed to get first serial number Error {e}")
            return None

    def delete_serial_number(self, serial_number):
        try:
            self.cursor.execute("""DELETE FROM queue where serial_number =?""", (serial_number,))
            self.connection.commit()
            logger.info(f"[+] Successful, Serial Number Deleted from the database")
        except Exception as e:
            logger.error(f"[-] Failed to delete serial number Error {e}")

    # endregion

    # region running data functions

    def save_running_data(self, serial_number, IHF_HEATING, SPG_HEATING, OXYGEN_HEATING,
                          PNG_PRESSURE, AIR_PRESSURE, DAAcetylenePressure):
        try:
            self.cursor.execute("""SELECT * FROM running_data WHERE serial_number = ?""",
                                (serial_number,))
            data = self.cursor.fetchone()
            if data:
                self.cursor.execute("""UPDATE running_data SET
                timestamp = ?, ihf_heating = ?, spg_heating = ?, oxygen_heating = ?,
                png_pressure = ?, air_pressure = ?, DAAcetylenePressure = ?
                WHERE serial_number = ?""",
                                    (time.time(), IHF_HEATING, SPG_HEATING, OXYGEN_HEATING, PNG_PRESSURE,
                                     AIR_PRESSURE, DAAcetylenePressure, serial_number))
            else:
                self.cursor.execute("""INSERT INTO running_data(timestamp, serial_number,
                ihf_heating, spg_heating, oxygen_heating, png_pressure, air_pressure, DAAcetylenePressure)
                VALUES(?,?,?,?,?,?,?,?)""",
                                    (time.time(), serial_number, IHF_HEATING, SPG_HEATING, OXYGEN_HEATING,
                                     PNG_PRESSURE, AIR_PRESSURE, DAAcetylenePressure))
            self.connection.commit()
            logger.info(f"[+] Successful, Running Data Saved to the database")
        except Exception as error:
            logger.error(f"[-] Failed to save running data. Error: {error}")

    # endregion

    # region Sync data TB database

    def add_sync_data(self, payload):
        try:
            self.cursor.execute("""SELECT * FROM sync_data
                                   WHERE date_=? AND shift=? AND serial_number=?""",
                                (payload['date_'], payload['shift'], payload['serial_number']))
            data = self.cursor.fetchone()
            payload_json = json.dumps(payload)  # Convert dictionary to JSON string

            if data:
                self.cursor.execute("""UPDATE sync_data SET payload=?
                                       WHERE date_=? AND shift=? AND serial_number=?""",
                                    (payload_json, payload['date_'], payload['shift'], payload['serial_number']))
            else:
                self.cursor.execute("""INSERT INTO sync_data (date_, shift, serial_number, payload)
                                       VALUES (?, ?, ?, ?)""",
                                    (payload['date_'], payload['shift'], payload['serial_number'], payload_json))

            self.connection.commit()
        except Exception as e:
            logger.error(f'ERROR {e} Sync Data not added to the database')

    def get_sync_data(self):
        try:
            self.cursor.execute('''SELECT payload FROM sync_data''')
            data = self.cursor.fetchall()
            # print(f"Sync_data: {data}")
            if data:
                return data
            else:
                return []
        except Exception as e:
            logger.error(f'ERROR {e} No Sync Data available')
            return []

    def delete_sync_data(self):
        try:
            # deleting the payload where ts is less than or equal to ts
            self.cursor.execute("""DELETE FROM sync_data """)
            self.connection.commit()
            logger.info(f"Successful, Deleted from sync_data database")
        except Exception as e:
            logger.info(f'Error in clear_sync_data {e} No sync Data to clear')

    ###################
    # COUNTER VALUES

    def insert_value(self, new_value):
        self.cursor.execute('INSERT INTO StoredValues (value) VALUES (?);', (new_value,))

    def get_and_clear_values(self):
        self.cursor.execute('SELECT SUM(value) AS total_sum FROM StoredValues;')
        total_sum = self.cursor.fetchone()[0]
        self.cursor.execute('DELETE FROM StoredValues;')
        self.cursor.close()
        return total_sum