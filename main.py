import paho.mqtt.client as mqtt
import requests
import time
from influxdb import InfluxDBClient

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    #client.subscribe("$SYS/#")
    client.subscribe("requests")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
  payload = msg.payload.decode('utf-8').strip()
  print(msg.topic+" "+str(msg.payload.decode('utf-8')))
  if(payload == 'weather/out'):
    w = get_weather()
    print("Publishing outside weather")
    client.publish('weather/out/temp',w['temperature'], retain=True)
    client.publish('weather/out/wind',w['windspeed'], retain=True)
    client.publish('weather/out/hum',w['humidity'], retain=True)

def get_domo():
  client = InfluxDBClient('192.168.0.3', 8086, '', '', 'domoticz')
  r = client.query("select value from device_outside_temperature limit 1")
  print(r)

def get_weather():
  res = requests.get("http://www.bom.gov.au/fwo/IDV60901/IDV60901.94870.json");
  r = res.json()
  ob = r['observations']['data'][0]
  temperature = ob['air_temp']
  windspeed = ob['wind_spd_kmh']
  return {'temperature': temperature, 'windspeed': windspeed, 'humidity': ob['rel_hum']}

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("192.168.0.3", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.

client.loop_start()

get_domo()

while True:
  print("Downloading weather data from BOM")
  w= get_weather()
  print("Publishing weather data")
  client.publish('weather/out/temp',w['temperature'], retain=True)
  client.publish('weather/out/wind',w['windspeed'], retain=True)
  client.publish('weather/out/hum',w['humidity'], retain=True)
  time.sleep(30)
