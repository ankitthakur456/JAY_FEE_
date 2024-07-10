import sqlite3
import time
import logging
import ast
import json
import datetime

logger = logging.getLogger('JayFee_log')


class DBHelper:
    def __init__(self):
        self.connection = sqlite3.connect("JAYFEE.db", check_same_thread=False)
        self.cursor = self.connection.cursor()
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_data_table(ts INTEGER, payload STRING)""")  # sync_data_table
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS queue(id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL, serial_number STRING)""")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS priority_queue(id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL, serial_number STRING)""")
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS
                                sync_data(
                                    serial_number VARCHAR(2),
                                    date_ STRING,
                                    shift VARCHAR(2),
                                    payload STRING)
                                """)
        self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS running_data(
                        timestamp INTEGER,
                        ihf_heating REAL,
                        spg_heating REAL,
                        oxygen_heating REAL,
                        png_pressure REAL,
                        air_pressure REAL,
                        daacetylene_pressure REAL,
                        spindle_speed REAL,
                        spindle_feed REAL,
                        serial_number TEXT PRIMARY KEY
                    )""")

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS breakdownData(breakId INTEGER NOT NULL,date_ DATE NOT NULL,
                                shift VARCHAR(1) NOT NULL,
                                time_ DATETIME, machine VARCHAR(20) NOT NULL, startTime DATETIME NOT NULL, stopTime DATETIME,
                                duration INTEGER,
                                PRIMARY KEY (breakId AUTOINCREMENT))''')  # BreakdownData table

    # region queue functions
    def enqueue_serial_number(self, serial_number: str):
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

    def enqueue_priority_serial(self, serial_number: str):
        try:
            self.cursor.execute("""SELECT serial_number FROM priority_queue where serial_number = ?""",
                                (serial_number,))
            if self.cursor.fetchone() is None:
                self.cursor.execute("""INSERT INTO priority_queue(serial_number, timestamp) VALUES(?,?)""",
                                    (serial_number, time.time()))
                self.connection.commit()
                logger.info(f"[+] Successful, Serial Number Enqueued to the database")
            else:
                logger.info(f"[-] Failed, Serial Number Already Enqueued to the database")
        except Exception as e:
            logger.error(f"[-] Failed to enqueue serial number Error {e}")

    def get_first_priority_serial(self):
        try:
            self.cursor.execute("""SELECT serial_number FROM priority_queue ORDER BY timestamp ASC LIMIT 1""")
            serial_number = self.cursor.fetchone()[0]
            if serial_number:
                return serial_number
            else:
                return None
        except Exception as e:
            logger.error(f"[-] Failed to get first serial number Error {e}")
            return None

    def delete_priority_serial(self, serial_number: str):
        try:
            self.cursor.execute("""DELETE FROM priority_queue where serial_number =?""", (serial_number,))
            self.connection.commit()
            logger.info(f"[+] Successful, Serial Number Deleted from the database")
        except Exception as e:
            logger.error(f"[-] Failed to delete serial number Error {e}")

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

    def delete_serial_number(self, serial_number: str):
        try:
            self.cursor.execute("""DELETE FROM queue where serial_number =?""", (serial_number,))
            self.connection.commit()
            logger.info(f"[+] Successful, Serial Number Deleted from the database")
        except Exception as e:
            logger.error(f"[-] Failed to delete serial number Error {e}")

    # endregion

    # region running data functions

    def save_running_data(self, serial_number, ihf_heating, spg_heating, oxygen_heating,
                          png_pressure, air_pressure, daacetylene_pressure, spindle_speed, spindle_feed):
        try:
            self.cursor.execute("SELECT * FROM running_data WHERE serial_number = ?", (serial_number,))
            data = self.cursor.fetchone()
            timestamp = int(time.time())

            if data:
                self.cursor.execute("""
                    UPDATE running_data SET
                        timestamp = ?, ihf_heating = ?, spg_heating = ?, oxygen_heating = ?,
                        png_pressure = ?, air_pressure = ?, daacetylene_pressure = ?, spindle_speed = ?, spindle_feed = ?
                    WHERE serial_number = ?""",
                                    (timestamp, ihf_heating, spg_heating, oxygen_heating, png_pressure,
                                     air_pressure, daacetylene_pressure, spindle_speed, spindle_feed, serial_number))
            else:
                self.cursor.execute("""
                    INSERT INTO running_data(timestamp, serial_number, ihf_heating, spg_heating, oxygen_heating,
                                             png_pressure, air_pressure, daacetylene_pressure, spindle_speed, spindle_feed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                    (timestamp, serial_number, ihf_heating, spg_heating, oxygen_heating,
                                     png_pressure, air_pressure, daacetylene_pressure, spindle_speed, spindle_feed))

            self.connection.commit()
            logger.info("[+] Successful, Running Data Saved to the database")
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

    # BREAKDOWN DATA HANDLING
    def addBreakDuration(self, mc_name):
        try:
            time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute('''SELECT startTime,breakId FROM breakdownData WHERE stopTime IS NULL AND machine=?''',
                                (mc_name,))
            startList = self.cursor.fetchall()
            logger.debug(startList)
            for items in startList:
                startTime = items[0]
                breakId = items[1]
                duration = (datetime.datetime.now() - datetime.datetime.strptime(startTime,
                                                                                 "%Y-%m-%d %H:%M:%S")).seconds
                self.cursor.execute('''UPDATE breakdownData SET time_=?,duration=?
                                   WHERE stopTime is NULL AND breakId=?''', (time_, duration, breakId))
            self.connection.commit()
            # self.cursor.execute('''DELETE FROM breakdownData WHERE duration<60 AND NOT stopTime is NULL''')
            # self.connection.commit()
            logger.debug('Successful' + 'Breakdown duration updated successfully in the database.')
        except Exception as e:
            logger.error('Error ' + str(e) + ' Could not add Breakdown duration to the database.')

    def addBreakStop(self, mc_name):
        try:
            time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute(
                '''SELECT startTime,breakId FROM breakdownData WHERE stopTime IS NULL and machine = ?''',
                (mc_name,))
            startList = self.cursor.fetchall()
            logger.debug(startList)
            for items in startList:
                startTime = items[0]
                breakId = items[1]
                if datetime.datetime.now() > datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S"):
                    duration = (datetime.datetime.now() - datetime.datetime.strptime(startTime,
                                                                                     "%Y-%m-%d %H:%M:%S")).seconds
                    self.cursor.execute('''UPDATE breakdownData SET time_=?, stopTime=?,duration=?
                                       WHERE stopTime is NULL AND breakId=?''', (time_, time_, duration, breakId))
                else:
                    stop_time = datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
                        seconds=10)
                    self.cursor.execute('''UPDATE breakdownData SET time_=?, stopTime=?,duration=?
                                       WHERE stopTime is NULL AND breakId=?''',
                                        (time_, stop_time, 10, breakId))
            self.connection.commit()
            logger.info(f'Successful: Breakdown is stopped successfully in the database for {mc_name}')
        except Exception as e:
            logger.error(f'Error: {e}, Could not stop Breakdown in the database for {mc_name}')

    def addBreakStart(self, mc_name, prevD, prevS, break_status):
        try:
            self.addBreakStop(mc_name)
            time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if break_status:
                self.cursor.execute("INSERT INTO breakdownData(date_,shift, time_, startTime, machine)"
                                    "VALUES (?,?,?,?,?)", (prevD, prevS, time_, time_, mc_name))
                self.connection.commit()
                logger.info(f'Successful: Breakdown is added successfully to the database for {mc_name}')
        except Exception as e:
            logger.error(f'Error: {e}, Could not add break down to the database for {mc_name}')

    def get_break_duration(self, mc_name, prevD, prevS):
        try:
            self.cursor.execute('''SELECT SUM(duration) FROM breakdownData
                              WHERE machine=? AND date_=? AND shift=? GROUP BY shift''', (mc_name, prevD, prevS))
            try:
                b_time = self.cursor.fetchone()
            except:
                b_time = [0]
            # logger.info(b_time)
            # logger.info(type(b_time))
            if b_time is None:
                return 0
            if type(b_time) is tuple:
                return b_time[0]
            else:
                return 0
        except Exception as e:
            logger.error(f'Error: {e}, Could not get daily breakdown for {mc_name}')
            return 0

    def get_current_breakdown_duration(self, mc_name, prevD, prevS):
        try:
            self.cursor.execute('''SELECT duration FROM breakdownData
                              WHERE machine=? AND date_=? AND shift=? AND stopTime is Null ORDER BY breakId DESC LIMIT 1''',
                                (mc_name, prevD, prevS))
            b_time = self.cursor.fetchone()
            if b_time is not None:
                return b_time[0]
            else:
                return 0
        except Exception as e:
            logger.error(f'Error: {e}, Could not get daily breakdown for {mc_name}')
            return 0

    def addBreakTime(self, today, shift, breakdown_sec):
        try:
            time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute("SELECT * FROM breakdownData WHERE date_=? ORDER BY date_ DESC LIMIT 1",
                                (today,))
            data = self.cursor.fetchall()
            if len(data) == 0:
                self.cursor.execute("INSERT INTO breakdownData(date_, time_, breakdown_time, breakdown_num)"
                                    "VALUES (?,?,?,?,?)", (today, shift, time_, breakdown_sec, 1))
            else:
                self.cursor.execute("UPDATE breakdownData SET breakdown_time=? WHERE date_=? AND shift=?",
                                    (breakdown_sec, today, shift))

            self.connection.commit()
            print('Successful', 'Breakdown is added successfully to the database.')
        except Exception:
            print('Error', 'Could not add Breakdown to the database.')

    def addBreakCount(self, today):
        try:
            time_ = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute("SELECT breakdown_num FROM breakdownData WHERE date_=? order by date_ DESC LIMIT 1",
                                (today,))
            bd_count = 0
            data = self.cursor.fetchall()
            if len(data) == 0:
                self.cursor.execute("INSERT INTO breakdownData(date_, time_, breakdown_time, breakdown_num)"
                                    "VALUES (?,?,?,?)", (today, time_, 0, 1))
            else:
                for row in data:
                    bd_count = row[0]
                self.cursor.execute("UPDATE breakdownData SET breakdown_num=? WHERE date_=?", (bd_count + 1, today))

            self.connection.commit()
            print('Successful', 'Breakdown is added successfully to the database.')
        except Exception:
            print('Error', 'Could not add Breakdown to the database.')

    def getBreakTime(self, today):
        self.cursor.execute("SELECT *  from breakdownData where date_=? ORDER BY date_ DESC LIMIT 1", (today,))
        try:
            data = self.cursor.fetchone()[0]
        except:
            data = 0
        return data
        # rows = self.cursor.fetchall()
        # self.connection.commit()
        # return rows

    def getBreakCount(self, today):
        self.cursor.execute("SELECT breakdown_num from breakdownData where date_=? ORDER BY date_ DESC LIMIT 1",
                            (today,))
        try:
            data = self.cursor.fetchone()[0]
        except:
            data = 0
        return data
        # rows = self.cursor.fetchall()
        # self.connection.commit()

    def get_breakCount(self, today, shift):
        try:
            self.cursor.execute("select count(breakId) from breakdownData where date_=? and shift=?",
                                (today, shift))
            break_count = self.cursor.fetchone()
            if break_count:
                return break_count[0]
            else:
                return 0
        except Exception as e:
            logger.error(f"[-] Unable to fetch break_count Error: {e}")
            return 0
