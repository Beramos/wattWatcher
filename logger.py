'''
    File name: logger.py
    Author: Bram De Jaegher
    Date created: 24/03/2021
    Date last modified: 24/03/2021
    Python Version: 3.6
'''

import sqlite3
import serial
import sys
import crcmod.predefined
import re
import pandas as pd

# Change your serial port here:
serialport = '/dev/ttyUSB0'

# How much data to store before writing to file (saving SD-card write cycles)
buffer_size=10

# Enable debug if needed:
debug = False

# Add/update OBIS codes here:
obiscodes = {
    "0-0:1.0.0": "TIMESTAMP",
    "0-0:96.3.10": "SWITCH_ELECTRICITY",
    "0-1:24.4.0": "SWITCH_GAS",
    "0-0:96.14.0": "DAY_OR_NIGHT",
    "1-0:1.8.1": "CONSUME_DAY_INT",
    "1-0:1.8.2": "CONSUME_NIGHT_INT",
    "1-0:2.8.1": "PRODUCE_DAY_INT",
    "1-0:2.8.2": "PRODUCE_NIGHT_INT",
    "1-0:1.7.0": "CONSUME",
    "1-0:2.7.0": "PRODUCE",
    "1-0:32.7.0": "VOLTAGE",
    "1-0:31.7.0": "CURRENT",
    "0-1:24.2.3": "GAS_CONSUME_INT"
    }

def checkcrc(p1telegram):
    # check CRC16 checksum of telegram and return False if not matching
    # split telegram in contents and CRC16 checksum (format:contents!crc)
    for match in re.compile(b'\r\n(?=!)').finditer(p1telegram):
        p1contents = p1telegram[:match.end() + 1]
        # CRC is in hex, so we need to make sure the format is correct
        givencrc = hex(int(p1telegram[match.end() + 1:].decode('ascii').strip(), 16))
    # calculate checksum of the contents
    calccrc = hex(crcmod.predefined.mkPredefinedCrcFun('crc16')(p1contents))
    # check if given and calculated match
    if debug:
        print(f"Given checksum: {givencrc}, Calculated checksum: {calccrc}")
    if givencrc != calccrc:
        if debug:
            print("Checksum incorrect, skipping...")
        return False
    return True


def parsetelegramline(p1line):
    # parse a single line of the telegram and try to get relevant data from it
    unit = ""
    timestamp = ""
    if debug:
        print(f"Parsing:{p1line}")
    # get OBIS code from line (format:OBIS(value)
    obis = p1line.split("(")[0]
    if debug:
        print(f"OBIS:{obis}")
    # check if OBIS code is something we know and parse it
    if obis in obiscodes:
        # get values from line.
        # format:OBIS(value), gas: OBIS(timestamp)(value)
        values = re.findall(r'\(.*?\)', p1line)
        value = values[0][1:-1]
        # timestamp requires removal of last char
        if obis == "0-0:1.0.0" or len(values) > 1:
            value = value[:-1]
        # report of connected gas-meter...
        if len(values) > 1:
            timestamp = value
            value = values[1][1:-1]
        # serial numbers need different parsing: (hex to ascii)
        if "96.1.1" in obis:
            value = bytearray.fromhex(value).decode()
        else:
            # separate value and unit (format:value*unit)
            lvalue = value.split("*")
            value = float(lvalue[0])
            if len(lvalue) > 1:
                unit = lvalue[1]
        # return result in tuple: description,value,unit,timestamp
        if debug:
            print (f"description:{obiscodes[obis]}, \
                     value:{value}, \
                     unit:{unit}")
        return (obiscodes[obis], value, unit)
    else:
        return ()

def init_dataFrame():
    columns = ["TIMESTAMP", "CONSUME_DAY_INT", "CONSUME_NIGHT_INT", 
        "PRODUCE_DAY_INT", "PRODUCE_NIGHT_INT", "DAY_OR_NIGHT",
        "CONSUME", "PRODUCE", "VOLTAGE", "CURRENT",
        "SWITCH_ELECTRICITY"  ,"SWITCH_GAS", "GAS_CONSUME_INT"]
    return pd.DataFrame(columns=columns)


def main():
    conn = sqlite3.connect("energy.db")
    ser = serial.Serial(serialport, 115200, xonxoff=1)
    p1telegram = bytearray()
    DF_logger = init_dataFrame()

    while True:
        try:
            # If buffer is full write to database
            if len(DF_logger) >= buffer_size:
                DF_logger.to_sql("data", conn, if_exists='append')
                DF_logger = init_dataFrame()

            # read input from serial port
            p1line = ser.readline()
            if debug:
                print ("Reading: ", p1line.strip())
            # P1 telegram starts with /
            # We need to create a new empty telegram
            if "/" in p1line.decode('ascii'):
                if debug:
                    print ("Found beginning of P1 telegram")
                p1telegram = bytearray()
                print('*' * 60 + "\n")
            # add line to complete telegram
            p1telegram.extend(p1line)
            # P1 telegram ends with ! + CRC16 checksum
            if "!" in p1line.decode('ascii'):
                if debug:
                    print("Found end, printing full telegram")
                    print('*' * 40)
                    print(p1telegram.decode('ascii').strip())
                    print('*' * 40)
                if checkcrc(p1telegram):
                    # parse telegram contents, line by line
                    output = []
                    output_dict = {}
                    for line in p1telegram.split(b'\r\n'):
                        r = parsetelegramline(line.decode('ascii'))
                        if r:
                            output.append(r)
                            output_dict[r[0]] = r[1]
                            if debug:
                                print(f"desc:{r[0]}, val:{r[1]}, u:{r[2]}")
                    DF_logger = DF_logger.append(output_dict, ignore_index=True)

        except KeyboardInterrupt:
            print("Stopping...")
            ser.close()
            break
        except:
            if debug:
                print(traceback.format_exc())
            # print(traceback.format_exc())
            print ("Something went wrong...")
            ser.close()
        # flush the buffer
        ser.flush()

if __name__ == '__main__':
    main()