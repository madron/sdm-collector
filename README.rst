sdm-collector
=============

sdm-collector collects data from Eastron SDM120 Modbus energy meter.


Installation
------------

::

    pip install sdm-collector


Usage
-----

Help::

    sdm-collector --help

Test run::

    sdm-collector --dump-data --one-shot

Run sdm-collector::

    sdm-collector --device /dev/ttyUSB0 --slaves 1
