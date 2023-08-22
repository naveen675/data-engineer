from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

import apache_beam as beam
import argparse

PROJECT_ID = '<YOUR_PROJECT_ID>'
SUBSCRIPTION = 'projects/' + PROJECT_ID + '/subscriptions/tweep-reader'
SCHEMA = 'created_at:TIMESTAMP,tweep_id:STRING,text:STRING,user:STRING,flagged:BOOLEAN'


def parse_pubsub(data):
    import json
    return json.loads(data)


def fix_timestamp(data):
    import datetime
    d = datetime.datetime.strptime(data['created_at'], "%d/%b/%Y:%H:%M:%S")
    data['created_at'] = d.strftime("%Y-%m-%d %H:%M:%S")
    return data


def check_tweep(data):
    BAD_WORDS = ['attack', 'drug', 'gun']
    data['flagged'] = False
    for word in BAD_WORDS:
        if word in data['text']:
            data['flagged'] = True
    return data


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION).with_output_types(bytes)
       | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
       | 'PubSubToJSON' >> beam.Map(parse_pubsub)
       | 'FixTimestamp' >> beam.Map(fix_timestamp)
       | 'CheckTweep' >> beam.Map(check_tweep)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:tweeper.tweeps'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()
    result.wait_until_finish()
