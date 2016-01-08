import argparse
import json
import serial
import time
import modbus_tk
import requests
from copy import copy
from datetime import datetime
from modbus_tk import modbus_rtu
from prometheus_client import start_http_server, Counter, Gauge
from redis import StrictRedis
from eastron import Sdm120

DEVICE = '/dev/ttyUSB0'
SLAVES = ['1']
BAUDRATE = 2400
BAUDRATE_CHOICES = (1200, 2400, 4800, 9600)
TIMEOUT = 1
ATTEMPTS = 1
DELAY = 0
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PREFIX = 'sdm'
EMONCMS_URL = 'http://emoncms.org'
EMONCMS_OFFSET = 0
EMONCMS_TIMEOUT = 3

sdm_elapsed_seconds = Gauge('sdm_elapsed_seconds', 'Collecting time in seconds')
sdm_successes = Counter('sdm_successes', 'Read successes', ['id', 'name'])
sdm_failures = Counter('sdm_failures', 'Read failures', ['id', 'name'])
sdm_voltage_volts = Gauge('sdm_voltage_volts', 'Voltage volts', ['id', 'name'])
sdm_current_amps = Gauge('sdm_current_amps', 'Current in ampere', ['id', 'name'])
sdm_power_watts = Gauge('sdm_power_watts', 'Power in watts', ['id', 'name'])
sdm_active_apparent_power_va = Gauge('sdm_active_apparent_power_va', 'Active apparent power in VA', ['id', 'name'])
sdm_reactive_apparent_power_var = Gauge('sdm_reactive_apparent_power_var', 'Reactive apparent power in VAr', ['id', 'name'])
sdm_power_factor = Gauge('sdm_power_factor', 'Power factor (cosfi)', ['id', 'name'])
sdm_frequency_hz = Gauge('sdm_frequency_hz', 'Frequency in Hertz', ['id', 'name'])
sdm_import_active_energy_wh = Counter('sdm_import_active_energy_wh', 'Import active energy in watthours', ['id', 'name'])
sdm_export_active_energy_wh = Counter('sdm_export_active_energy_wh', 'Export active energy in watthours', ['id', 'name'])
sdm_total_active_energy_wh = Counter('sdm_total_active_energy_wh', 'Total active energy in watthours', ['id', 'name'])


def parse_slaves(slave_list):
    slaves = []
    slave = dict()
    for text in slave_list:
        if text.isdigit():
            if slave:
                slaves.append(slave)
            slave = dict(id=int(text), name='')
        else:
            if slave:
                slave['name'] = text
                slaves.append(slave)
                slave = dict()
    if slave:
        slaves.append(slave)
    return slaves


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


def emoncsm_post(emoncms=dict(), slave=None, verbosity=0):
    if emoncms.get('key', None):
        url = '%s/input/post.json' % emoncms['url']
        if slave.data:
            data = copy(slave.data)
            data['import_active_energy_kwh'] = data['import_active_energy_wh'] / 1000
            data['export_active_energy_kwh'] = data['export_active_energy_wh'] / 1000
            data['total_active_energy_kwh'] = data['total_active_energy_wh'] / 1000
            payload = dict(
                apikey=emoncms['key'],
                node=slave.id,
                json=json.dumps(data)
            )
            response = requests.get(url, params=payload, timeout=emoncms['timeout'])
            if verbosity >= 1:
                print('Slave %d - Emoncms status code: %d' % (slave.id, response.status_code))


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
    pipe.hset(key, 'name', slave.name)
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


def set_prometheus_metrics(slave):
    labels = dict(id=slave.id, name=slave.name)
    if slave.read_success:
        sdm_successes.labels(labels).inc()
        sdm_voltage_volts.labels(labels).set(slave.data['voltage_volts'])
        sdm_current_amps.labels(labels).set(slave.data['current_amps'])
        sdm_power_watts.labels(labels).set(slave.data['power_watts'])
        sdm_active_apparent_power_va.labels(labels).set(slave.data['active_apparent_power_va'])
        sdm_reactive_apparent_power_var.labels(labels).set(slave.data['reactive_apparent_power_var'])
        sdm_power_factor.labels(labels).set(slave.data['power_factor'])
        sdm_frequency_hz.labels(labels).set(slave.data['frequency_hz'])
        sdm_import_active_energy_wh.labels(labels)._value._value = slave.data['import_active_energy_wh']
        sdm_export_active_energy_wh.labels(labels)._value._value = slave.data['export_active_energy_wh']
        sdm_total_active_energy_wh.labels(labels)._value._value = slave.data['total_active_energy_wh']
    else:
        sdm_failures.labels(labels).inc(slave.read_failures)


def collect(master, redis, emoncms=dict(), slaves=[], attempts=ATTEMPTS, dump_data=False, verbosity=0):
    info = dict(read_successes=0, read_failures=0)
    start = datetime.now()
    slave_list = []
    for s in slaves:
        slave = Sdm120(master=master, id=s['id'], name=s['name'])
        slave.read_data(attempts=attempts, verbosity=verbosity)
        slave_list.append(slave)
        if slave.read_success:
            info['read_successes'] += 1
        else:
            info['read_failures'] += 1
        if dump_data:
            print(slave)
        # Emoncms
        emoncsm_post(emoncms=emoncms, slave=slave, verbosity=verbosity)
        # Redis
        if redis:
            redis_save_slave(redis=redis, slave=slave)
        # Prometheus
        set_prometheus_metrics(slave)
    info['elapsed_seconds'] = (datetime.now() - start).total_seconds()
    # Redis
    if redis:
        redis_save_info(redis=redis, info=info)
    # Prometheus
    sdm_elapsed_seconds.set(info['elapsed_seconds'])
    sdm_successes.labels('', '').inc(info['read_successes'])
    sdm_failures.labels('', '').inc(info['read_failures'])


def main():
    parser = argparse.ArgumentParser(description='Eastron SDM120 Modbus collector.')
    parser.add_argument('--device', metavar='DEV', type=str, default=DEVICE,
                        help='Serial device (default: %s)' % DEVICE)
    parser.add_argument('--slaves', metavar='ID', type=str, nargs='+',
                        default=SLAVES,
                        help='Slave list (default: %s)' % SLAVES)
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
    # Redis
    parser.add_argument('--redis-host', metavar='HOST', type=str, help='Redis host')
    parser.add_argument('--redis-port', metavar='PORT', type=int, default=REDIS_PORT,
                        help='Redis port (default: %d)' % REDIS_PORT)
    parser.add_argument('--redis-db', metavar='DB', type=int, default=REDIS_DB,
                        help='Redis db (default: %d)' % REDIS_DB)
    parser.add_argument('--redis-prefix', metavar='PRFX', type=str, default=REDIS_PREFIX,
                        help='Redis prefix (default: %s)' % REDIS_PREFIX)
    # Emoncms
    parser.add_argument('--emoncms-url', metavar='URL', type=str, default=EMONCMS_URL,
                        help='Emoncms url (default: %s' % EMONCMS_URL)
    parser.add_argument('--emoncms-offset', metavar='OFFSET', type=int, default=EMONCMS_OFFSET,
                        help='Emoncms time offset (default: %d' % EMONCMS_OFFSET)
    parser.add_argument('--emoncms-apikey', metavar='KEY', type=str,
                        help='Emoncms api write key')
    parser.add_argument('--emoncms-timeout', metavar='SECONDS', type=float,
                        help='Emoncms request timeout (default: %d' % EMONCMS_TIMEOUT)
    # Prometheus
    parser.add_argument('--prometheus-port', metavar='PORT', type=int,
                        help='Prometheus metrics port')

    args = parser.parse_args()
    slaves = parse_slaves(args.slaves)
    if args.verbosity >= 1:
        print(args)
        print(slaves)

    master = get_master(device=args.device, baudrate=args.baudrate, timeout=args.timeout, verbosity=args.verbosity)
    redis = None
    if args.redis_host:
        redis = StrictRedis(host=args.redis_host, port=args.redis_port, db=args.redis_db)
        redis.var_name_prefix = args.redis_prefix
        redis_save_slaves(redis=redis, slaves=args.slaves)
    emoncms = dict(url=args.emoncms_url, offset=args.emoncms_offset, key=args.emoncms_apikey, timeout=args.emoncms_timeout)
    if args.prometheus_port:
        start_http_server(args.prometheus_port)
    while True:
        collect(master, redis, emoncms=emoncms, slaves=slaves, attempts=args.attempts, dump_data=args.dump_data, verbosity=args.verbosity)
        if args.one_shot:
            exit(0)
        time.sleep(args.delay)
