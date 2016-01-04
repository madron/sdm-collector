import argparse
import serial
import time
import modbus_tk
from datetime import datetime
from modbus_tk import modbus_rtu
from redis import StrictRedis
from eastron import Sdm120

DEVICE = '/dev/ttyUSB0'
SLAVES = [1]
BAUDRATE = 2400
BAUDRATE_CHOICES = (1200, 2400, 4800, 9600)
TIMEOUT = 1
ATTEMPTS = 1
DELAY = 0
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PREFIX = 'sdm'


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


def redis_save_slaves(redis=None, slaves=[]):
    key = '%s:slaves' % redis.var_name_prefix
    pipe = redis.pipeline()
    pipe.delete(key)
    for slave_id in slaves:
        pipe.rpush(key, slave_id)
    pipe.execute()


def redis_save_slave(redis=None, slave=None):
    key = '%s:%d' % (redis.var_name_prefix, slave.id)
    pipe = redis.pipeline()
    if slave.data:
        for k, v in slave.data.iteritems():
            pipe.hset(key, k, v)
        pipe.hincrby(key, 'read_successes', 1)
        pipe.hincrby(key, 'read_failures', 0)
    else:
        pipe.hincrby(key, 'read_successes', 0)
        pipe.hincrby(key, 'read_failures', 1)
    pipe.execute()


def redis_save_info(redis=None, info=dict()):
    key = '%s:info' % redis.var_name_prefix
    pipe = redis.pipeline()
    pipe.hset(key, 'elapsed_seconds', info['elapsed_seconds'])
    pipe.hincrby(key, 'read_successes', info['read_successes'])
    pipe.hincrby(key, 'read_failures', info['read_failures'])
    pipe.execute()


def collect(master, redis, slaves=SLAVES, attempts=ATTEMPTS, dump_data=False, verbosity=0):
    info = dict(read_successes=0, read_failures=0)
    start = datetime.now()
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
        if data:
            info['read_successes'] += 1
        else:
            info['read_failures'] += 1
        if dump_data:
            print('--- Slave %d' % slave_id)
            for name, address in slave.REGISTERS:
                print name, data.get(name, '')
        if redis:
            redis_save_slave(redis=redis, slave=slave)
    info['elapsed_seconds'] = (datetime.now() - start).total_seconds()
    if redis:
        redis_save_info(redis=redis, info=info)


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
    parser.add_argument('--redis-host', metavar='HOST', type=str, help='Redis host')
    parser.add_argument('--redis-port', metavar='PORT', type=int, default=REDIS_PORT,
                        help='Redis port (default: %d)' % REDIS_PORT)
    parser.add_argument('--redis-db', metavar='DB', type=int, default=REDIS_DB,
                        help='Redis db (default: %d)' % REDIS_DB)
    parser.add_argument('--redis-prefix', metavar='PRFX', type=str, default=REDIS_PREFIX,
                        help='Redis prefix (default: %s)' % REDIS_PREFIX)
    parser.add_argument("-v", "--verbosity", action="count",
                        help="Increase output verbosity")

    args = parser.parse_args()
    if args.verbosity >= 1:
        print(args)

    master = get_master(device=args.device, baudrate=args.baudrate, timeout=args.timeout, verbosity=args.verbosity)
    redis = None
    if args.redis_host:
        redis = StrictRedis(host=args.redis_host, port=args.redis_port, db=args.redis_db)
        redis.var_name_prefix = args.redis_prefix
        redis_save_slaves(redis=redis, slaves=args.slaves)
    while True:
        collect(master, redis, slaves=args.slaves, attempts=args.attempts, dump_data=args.dump_data, verbosity=args.verbosity)
        if args.one_shot:
            exit(0)
        time.sleep(args.delay)
