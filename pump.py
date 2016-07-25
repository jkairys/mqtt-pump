import requests
import paho.mqtt.client as mqtt
import time
import logging
from influxdb import InfluxDBClient


logger = logging.getLogger('pump')
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add ch to logger
ch.setFormatter(formatter)
logger.addHandler(ch)

bom_stations = {
  'moorabin': "http://www.bom.gov.au/fwo/IDV60901/IDV60901.94870.json",
  'falls-creek': "http://www.bom.gov.au/fwo/IDV60801/IDV60801.94903.json"
}


def on_connect(client, userdata, flags, rc):
    logger.info("MQTT connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("$SYS/#")
    #client.subscribe("requests")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
  payload = msg.payload.decode('utf-8').strip()
  logger.debug("MQTT message: "+msg.topic+" "+str(msg.payload.decode('utf-8')))


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("192.168.0.3", 1883, 60)
# start background thread
client.loop_start()



def get_weather_bom(url):
  res = requests.get(url);
  r = res.json()
  ob = r['observations']['data'][0]
  #print(ob)
  return {
    'temperature': float(ob['air_temp']),
    'windspeed': int(ob['wind_spd_kmh']),
    'humidity': int(ob['rel_hum']),
    'rainfall': float(ob['rain_trace'])
  }

def get_weather():
  for station in bom_stations:
    url = bom_stations[station]
    r = get_weather_bom(url)
    logger.debug("got {0}".format(r))
    logger.info(
      "{station:s}: {temperature:.1f}*C, {humidity:d}% hum. {windspeed:d}km/h with {rainfall:.1f}mm rain.".format(
        station=station, temperature=r['temperature'], humidity=r['humidity'], windspeed=r['windspeed'], rainfall=r['rainfall']
      )
    )
    for measure in r:
      topic = "weather/{station}/{measure}".format(station=station, measure=measure)
      value = r[measure]

      logger.info("Publishing to {topic}={value}".format(topic=topic, value=value))
      client.publish(topic,value, retain=True)

get_weather()