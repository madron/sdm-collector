import modbus_tk


class Sdm120(object):
    REGISTERS = [
        ('voltage_volts', 0x00),
        ('current_amps', 0x06),
        ('power_watts', 0x0C),
        ('active_apparent_power_va', 0x12),
        ('reactive_apparent_power_var', 0x18),
        ('power_factor', 0x1E),
        ('frequency_hz', 0x46),
        ('import_active_energy_wh', 0x48),
        ('export_active_energy_wh', 0x4C),
        ('total_active_energy_wh', 0x0158),
    ]

    def __init__(self, master=None, id=1, name=''):
        self.master = master
        self.id = id
        self.name = name
        self.data = dict()
        self.read_failures = 0
        self.read_success = False

    def read_data(self, attempts=1, verbosity=0):
        registers = []
        chunks = [
            dict(address=0x00, registers=32),
            dict(address=0x46, registers=8),
            dict(address=0x0158, registers=2),
        ]
        attempt = 0
        while attempt < attempts:
            attempt += 1
            if verbosity >= 1:
                print('Attempt %d' % attempt)
            try:
                for chunk in chunks:
                    missing = chunk['address'] / 2 - len(registers)
                    if missing:
                        registers += [None for x in range(missing)]
                    registers += self.master.execute(
                        self.id, modbus_tk.defines.READ_INPUT_REGISTERS,
                        chunk['address'], chunk['registers'],
                        data_format='>' + 'f' * (chunk['registers'] / 2)
                    )
                # Debug
                # for i, value in enumerate(registers):
                #     if value is not None:
                #         print('%s - %s' % (hex(i * 2), value))
                self.data = dict()
                for name, address in self.REGISTERS:
                    self.data[name] = registers[address / 2]
                self.data['import_active_energy_wh'] = self.data['import_active_energy_wh'] * 1000
                self.data['export_active_energy_wh'] = self.data['export_active_energy_wh'] * 1000
                self.data['total_active_energy_wh'] = self.data['total_active_energy_wh'] * 1000
                self.read_success = True
                break
            except (modbus_tk.modbus.ModbusError, modbus_tk.modbus.ModbusInvalidResponseError):
                self.read_failures += 1

    def __str__(self):
        if self.name:
            lines = ['--- Slave %d (%s)' % (self.id, self.name)]
        else:
            lines = ['--- Slave %d' % self.id]
        for name, address in self.REGISTERS:
            lines.append('%s, %s' % (name, self.data.get(name, '')))
        return '\n'.join(lines)
