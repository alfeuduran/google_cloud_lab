import apache_beam as beam
import csv

def addtimezone(lat, lon):
   try:
      import timezonefinder
      tf = timezonefinder.TimezoneFinder()
      return (lat, lon, tf.timezone_at(lng=float(lon), lat=float(lat)))
      #return (lat, lon, 'America/Los_Angeles') # FIXME
   except ValueError:
      return (lat, lon, 'TIMEZONE') # header

def as_utc(date, hhmm, tzone):
   try:
      if len(hhmm) > 0 and tzone is not None:
         import datetime, pytz
         loc_tz = pytz.timezone(tzone)
         loc_dt = loc_tz.localize(datetime.datetime.strptime(date,'%Y-%m-%d'), is_dst=False)
         # can't just parse hhmm because the data contains 2400 and the like ...
         loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
         utc_dt = loc_dt.astimezone(pytz.utc)
         return utc_dt.strftime('%Y-%m-%d %H:%M:%S')
      else:
         return '' # empty string corresponds to canceled flights
   except ValueError as e:
      print '{} {} {}'.format(date, hhmm, tzone)
      raise e

def tz_correct(line, airport_timezones):
   fields = line.split(',')
   if fields[0] != 'FL_DATE' and len(fields) == 27:
      # convert all times to UTC
      dep_airport_id = fields[6]
      arr_airport_id = fields[10]
      dep_timezone = airport_timezones[dep_airport_id][2]
      arr_timezone = airport_timezones[arr_airport_id][2]

      for f in [13, 14, 17]: #crsdeptime, deptime, wheelsoff
         fields[f] = as_utc(fields[0], fields[f], dep_timezone)
      for f in [18, 20, 21]: #wheelson, crsarrtime, arrtime
         fields[f] = as_utc(fields[0], fields[f], arr_timezone)

      yield ','.join(fields)

if __name__ == '__main__':
   with beam.Pipeline('DirectRunner') as pipeline:

      airports = (pipeline
         | 'airports:read' >> beam.io.ReadFromText('simulate/airports.csv.gz')
         | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
         | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
      )

      flights = (pipeline
         | 'flights:read' >> beam.io.ReadFromText('simulate/201501_part.csv')
         | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
      )

      flights | beam.io.textio.WriteToText('all_flights')

      pipeline.run()