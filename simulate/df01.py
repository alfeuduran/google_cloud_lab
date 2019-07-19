import apache_beam as beam
import csv

if __name__ == '__main__':
   with beam.Pipeline('DirectRunner') as pipeline:

      airports = (pipeline
         | beam.io.ReadFromText('simulate/airports.csv.gz')
         | beam.Map(lambda line: next(csv.reader([line])))
         | beam.Map(lambda fields: (fields[0], (fields[21], fields[26])))
      )

      # COD Aeroporto, LAT, LONG
      # 10000101,58.109444444,-152.906666667

      airports | beam.Map(lambda (airport, data): '{},{}'.format(airport, ','.join(data)) )| beam.io.textio.WriteToText('extracted_airports')
      

      pipeline.run()


