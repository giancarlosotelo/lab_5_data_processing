# Based on mnistPubSub.py from the MS3 repository, with the ML model replaced by Filter and Convert.

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def has_all_measurements(record):
    # Keep the record only if none of its values are missing
    return all(value is not None for value in record.values())


def convert_units(record):
    record['pressure'] = record['pressure'] / 6.895           # kPa -> psi
    record['temperature'] = record['temperature'] * 1.8 + 32  # Celsius -> Fahrenheit
    return record


def run(argv=None):
  parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--input', dest='input', required=True,
                      help='Input Pub/Sub topic to read the readings from.')
  parser.add_argument('--output', dest='output', required=True,
                      help='Output Pub/Sub topic to write the converted readings to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=pipeline_options) as p:
    (p | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=known_args.input)
       | 'toDict' >> beam.Map(lambda x: json.loads(x))
       | 'Filter' >> beam.Filter(has_all_measurements)
       | 'Convert' >> beam.Map(convert_units)
       | 'to byte' >> beam.Map(lambda x: json.dumps(x).encode('utf8'))
       | 'Write to PubSub' >> beam.io.WriteToPubSub(topic=known_args.output))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
