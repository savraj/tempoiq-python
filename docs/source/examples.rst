Examples
========

Initializing the client
-----------------------

You must initialize a client object before you can make any API calls::

    import tempoiq.session
    from tempoiq.client import Client

    endpoint = HTTPEndpoint("https://your-url.backend.tempoiq.com",
                            "your-key", 
                            "your-secret")
    client = Client(endpoint)


Creating a device
-----------------

To create a new device in your backend::

    from tempoiq.protocol.device import Device
    from tempoiq.protocol.sensor import Sensor
    import tempoiq.response

    temp_sensor = Sensor("temperature", attributes={"unit": "degC"})
    humid_sensor = Sensor("humidity", attributes={"unit": "percent"})

    device = Device("thermostat-12345", 
                    attributes={"type": "thermostat", "building": "24"},
                    sensors=[temp_sensor, humid_sensor])
    response = client.create_device(device)

    if response.successful != tempoiq.response.SUCCESS:
        print("Error creating device!")


Writing sensor data
-------------------

To write sensor data for the device we created above::

    import datetime
    from tempoiq.protocol.point import Point

    current = datetime.datetime.now()

    device_data = {"temperature": [ Point(current, 23.5) ],
                   "humidity": [ Point(current, 72.0) ]}

    response = client.write({"thermostat-12345": device_data})

    if res.successful != tempoiq.response.SUCCESS:
        print("Error writing data!")


Getting devices
---------------

To get a list of all devices matching given filter criteria::

    result = client.query(Device).filter(Device.attributes["building"] == "24").read()

    for dev in result.data["data"]:
        print("Got device with key: {}".format(dev.key))


You can use a compound selector to filter for the logical AND or OR of several
selectors::

    from tempoiq.protocol.query.selection import and_, or_

    result = client.query(Device)
                   .filter(or_(
                        [
                            Device.attributes["building"] == "24",
                            Device.attributes["building"] == "26"
                        ]
                    )
                   .read()


Reading sensor data
-------------------

To read raw data from one or more sensors and devices::

    start = datetime.datetime(2014, 6, 1, 0, 0)
    end = datetime.datetime(2014, 6, 2, 0, 0)

    result = client.query(Sensor).filter(Device.attributes["building"] == "24")
                                 .filter(Sensor.key == "temperature")
                                 .read(start=start, end=end)

    for row in result.data:
        print("Timestamp: {} Values: {}".format(row.timestamp, row.values))

Rollups, interpolation, and aggregation can be added to the query as well::

    result = client.query(Sensor).filter(Device.attributes["building"] == "24")
                                 .rollup("max", "1hour")
                                 .read(start=start, end=end)