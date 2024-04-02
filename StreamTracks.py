"""A Project"""

import argparse
import json
import logging
import os

import apache_beam as beam
import tensorflow as tf
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class VelocityAverageFn(beam.CombineFn):
  def create_accumulator(self):
    second = -1    
    sum_x_pos = 0.0
    sum_x_neg = 0.0    
    sum_y_pos = 0.0
    sum_y_neg = 0.0     
    count = 0
    accumulator = second, sum_x_pos, sum_x_neg, sum_y_pos, sum_y_neg, count
    return accumulator

  def add_input(self, accumulator, input):
    second, sum_x_pos, sum_x_neg, sum_y_pos, sum_y_neg, count = accumulator
    second = input['frame'] // 25
    xVelocity = input['xVelocity']
    yVelocity = input['yVelocity']
    if xVelocity > 0:
       sum_x_pos + xVelocity
    else:
       sum_x_neg + xVelocity
    if yVelocity > 0:
       sum_y_pos + yVelocity
    else:
       sum_y_neg + yVelocity
    return second, sum_x_pos, sum_x_neg, sum_y_pos, sum_y_neg, count + 1

  def merge_accumulators(self, accumulators):
    second, sum_x_pos, sum_x_neg, sum_y_pos, sum_y_neg, count = zip(*accumulators)
    return second[0], sum(sum_x_pos), sum(sum_x_neg), sum(sum_y_pos), sum(sum_y_neg), sum(count)

  def extract_output(self, accumulator):
    second, sum_x_pos, sum_x_neg, sum_y_pos, sum_y_neg, count = accumulator
    if count == 0:
      return float('NaN')
    result = {}
    result['second'] = second        
    result['avg_x_velocity_pos'] = sum_x_pos / count
    result['avg_x_velocity_neg'] = sum_x_neg / count
    result['avg_y_velocity_pos'] = sum_y_pos / count
    result['avg_y_velocity_neg'] = sum_y_neg / count

    return [result]
  
class ResultDoFn(beam.DoFn):
  def process(self, element):
    if not element:
      result = {}
      result['error'] = "Not a valid time selection"
      return [result]
    result = {}
    result['second'] = element['second']   

    if element['avg_x_velocity_pos'] < 30 and element['avg_x_velocity_pos'] > 0:
       result['east'] = "Traffic slowdown going east on this highway"
    else:
      result['east'] = "Traffic is smooth going east on this highway"  
    if element['avg_x_velocity_pos'] > -30 and element['avg_x_velocity_pos'] < 0:
      result['west'] = "Traffic slowdown going west on this highway"
    else:
      result['west'] = "Traffic is smooth going west on this highway"    
    return [result]
      
def run(argv=None):
  parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--input', dest='input', required=True,
                      help='Input track file to process.')
  parser.add_argument('--output', dest='output', required=True,
                      help='Output file to write average velocities to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True;
  with beam.Pipeline(options=pipeline_options) as p:
        fromPubSub = (p | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=known_args.input)
                        | "toDict" >> beam.Map(lambda x: json.loads(x)))
        createRequest = fromPubSub | 'Create Request' >> beam.Map(lambda data: beam.io.ReadFromBigQueryRequest(query='SELECT * FROM Tracks.track_'+"{:02d}".format(data['track']) + '_output WHERE second ='+ str(data['second'])))
        # ' + f"{str(data['track']):02d}" + '
        readBq = createRequest | 'Read from big query' >> beam.io.ReadAllFromBigQuery()
        
        result = readBq | 'Generate result' >> beam.ParDo(ResultDoFn())   
        toByte = result | 'To Byte' >> beam.Map(lambda x: json.dumps(x).encode('utf8'))

        toByte | 'Send to Sub' >> beam.io.WriteToPubSub(topic=known_args.output)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()