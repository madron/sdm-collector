#!/usr/bin/env python

import argparse
import os
import serial
import time
import modbus_tk
from modbus_tk import modbus_rtu
from eastron import Sdm120

DEVICE = '/dev/ttyUSB0'
SLAVES = [1]
BAUDRATE = 2400
BAUDRATE_CHOICES = (1200, 2400, 4800, 9600)
TIMEOUT = 1
ATTEMPTS = 1
DELAY = 0


def loop(master, n=1):
    slave = Sdm120(master=master, slave=1)
    for i in range(n):
        data = slave.get_data()
        os.system('clear')
        print i
        for name, address in slave.REGISTERS:
            print name, data[name]


def get_master(device=DEVICE, baudrate=BAUDRATE, timeout=TIMEOUT, verbosity=0):
    if verbosity >= 2:
        modbus_tk.utils.create_logger('console')
    master = modbus_rtu.RtuMaster(
        serial.Serial(
            device,
            baudrate=baudrate,
            bytesize=8,
            parity='N',
            stopbits=1,
            xonxoff=0,
        )
    )
    master.set_timeout(timeout)
    if verbosity >= 2:
        master.set_verbose(True)
    return master


def collect(master, slaves=SLAVES, attempts=ATTEMPTS, dump_data=False, verbosity=0):
    for slave_id in slaves:
        slave = Sdm120(master=master, id=slave_id)
        remaining_attempts = attempts
        data = dict()
        while remaining_attempts:
            if verbosity >= 1:
                print('Remaining attempts: %d' % remaining_attempts)
            remaining_attempts -= 1
            try:
                data = slave.get_data()
            except (modbus_tk.modbus.ModbusError, modbus_tk.modbus.ModbusInvalidResponseError):
                pass
        if dump_data:
            print('--- Slave %d' % slave_id)
            for name, address in slave.REGISTERS:
                print name, data.get(name, '')


def main():
    parser = argparse.ArgumentParser(description='Eastron SDM120 Modbus collector.')
    parser.add_argument('--device', metavar='DEV', type=str, default=DEVICE,
                        help='Serial device (default: %s)' % DEVICE)
    parser.add_argument('--slaves', metavar='ID', type=int, nargs='+',
                        default=SLAVES,
                        help='Slave id list (default: %s)' % SLAVES)
    parser.add_argument('--baudrate', metavar='BPS', type=int,
                        choices=BAUDRATE_CHOICES, default=BAUDRATE,
                        help='Baudrate (default: %s)' % BAUDRATE)
    parser.add_argument('--timeout', metavar='SECONDS', type=float, default=TIMEOUT,
                        help='Timeout in seconds (default: %s)' % TIMEOUT)
    parser.add_argument('--attempts', metavar='N', type=int, default=ATTEMPTS,
                        help='Read attempts (default: %d)' % ATTEMPTS)
    parser.add_argument('--delay', metavar='SECONDS', type=int, default=DELAY,
                        help='Polling delay in seconds (default: %s)' % DELAY)
    parser.add_argument('--dump-data', action='store_true',
                        help='Print collected data on standard output')
    parser.add_argument('--one-shot', action='store_true',
                        help='Collect data one time and exit')
    parser.add_argument("-v", "--verbosity", action="count",
                        help="Increase output verbosity")

    args = parser.parse_args()
    if args.verbosity >= 1:
        print(args)

    master = get_master(device=args.device, baudrate=args.baudrate, timeout=args.timeout, verbosity=args.verbosity)
    while True:
        collect(master, slaves=args.slaves, attempts=args.attempts, dump_data=args.dump_data, verbosity=args.verbosity)
        if args.one_shot:
            exit(0)
        time.sleep(args.delay)
