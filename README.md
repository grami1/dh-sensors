# dh-sensors

dh-sensors project is a part of the [Dihome project](https://github.com/grami1/dihome). 
It contains scripts for getting data from sensors connected to Raspberry Pi.

## How to run
1. Create AWS IoT core Thing (see the [guide](https://docs.aws.amazon.com/iot/latest/developerguide/create-iot-resources.html))
2. Download device certificate, private key and AWS Root CA certificate
3. Create .env file based on the ``env-template`` and setup variables
4. Run ``python3 dht22_iot_core.py``
