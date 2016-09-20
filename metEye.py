import requests
import pprint
from dateutil import parser
import logging, datetime

pp = pprint.PrettyPrinter(indent=2)
logger = logging.getLogger('mqtt-pubnub-bridge')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add ch to logger
ch.setFormatter(formatter)
logger.addHandler(ch)


def forecast(state, suburb):
  result = requests.get("http://www.bom.gov.au/places/{state}/{suburb}/forecast/detailed/".format(state=state, suburb=suburb))
  txt = result.text

  # Sample file for testing
  # f = open("bomforecast.html", 'r')
  # txt = f.read()
  # print(txt)

  from lxml import html
  doc = html.fromstring(txt).cssselect("#main-content")
  doc = doc[0]

  days = doc.cssselect(".forecast-day")
  weather = {}

  # mapping of BOM text to our short form
  # if not used, leave as None
  _map = {
    '10% chance of more than (mm)': None,
    '25% chance of more than (mm)': None,
    '50% chance of more than (mm)': 'rain_mm',
    'Air temperature (°C)':'temperature',
    'Chance of any rain': 'rain_probability',
    'Dew point temperature (°C)': None,
    'Feels like (°C)': None,
    'Fog': None,
    'Forest fuel dryness factor': None,
    'Frost': None,
    'Mixing height (m)': None,
    'Rain': None,
    'Relative humidity (%)': 'humidity',
    'Snow': None,
    'Thunderstorms': None,
    'UV Index': None,
    'Wind direction': 'wind_direction',
    'Wind speed  ': 'wind_speed'
  }

  tnow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

  for d in days:
    tables = d.cssselect("table")
    h2 = d.cssselect("h2")

    if(not len(h2)):
      continue

    tmp = h2[0].text.split(" ")
    date_str = " ".join(tmp[1:])

    dt = parser.parse(date_str)
    date_str = dt.strftime("%Y-%m-%d")
    #print(date_str)
    #weather[date_str] = {}
    for t in tables:
      measure = t.attrib['summary']

      thead = t.cssselect("thead")

      colNames = []
      colNum = 0
      for th in thead[0].cssselect("th"):
        colNum = colNum + 1
        if(colNum == 1):
          continue
        colNames.append(th.text)
        #weather[date_str][th.text] = None

      #print(colNames)

      tbody = t.cssselect("tbody")

      for row in tbody[0].cssselect("tr"):
        th = row.cssselect("th")
        measure = th[0].text
        # map this to something shorter
        measure = _map[measure]
        if(measure is None):
          continue
        cols = row.cssselect("td")
        if(not len(cols)):
          continue
        #print(measure)

        vals = []
        valNum = 0
        for c in cols:
          #colname is time
          colName = colNames[valNum]
          valNum = valNum + 1
          #print(colName)
          dt =  parser.parse(date_str + " " + colName).strftime("%Y-%m-%d %H:%M:%S")
          if(dt < tnow):
            continue
          if dt not in weather:
            weather[dt] = {}
          val = c.text
          if(val is not None and "%" in val):
            val = int(val.replace("%","")) / 100
          if(val == "–" or val == "c.text"):
            val = None
          weather[dt][measure] = val

  return weather
  #pp.pprint(weather)
  """
  all_ts = {}
  all_measures = {}
  for ts in weather:
    if(ts not in all_ts):
      all_ts[ts] = True
    for (measure, val) in ts.items():
      if(measure not in all_measures):
        all_measures[measure] = True
  all_ts = all_ts.keys()
  all_ts.sort()
  all_measures = all_measures.keys()
  all_measures.sort()
  tmp = copy.copy(weather)
  weather = {}
  for ts in all_ts:
  """

if __name__ == "__main__":
  from pubnub import Pubnub
  logger.info("Connecting to PubNub")
  pubnub = Pubnub(
    publish_key="pub-c-fb55ae11-11cd-4792-ada4-4ec898c0ebc5",
    subscribe_key="sub-c-8b96bdca-7e49-11e6-b27b-02ee2ddab7fe"
  )

  logger.info("Getting weather forecasts from BoM MetEye")
  stations = [{"state":"vic", "suburb":"chelsea"}]
  for st in stations:
    logger.info("Getting forecast for {state}/{suburb}".format(state=st['state'], suburb=st['suburb']))
    fc = forecast(st["state"], st["suburb"])
    pp.pprint(fc)
    ch = "weather-forecast/{state}/{suburb}".format(state=st['state'], suburb=st['suburb'])
    logger.info("Pushing forecast to PubNub {:}".format(ch))
    pubnub.publish(channel=ch, message=fc)
