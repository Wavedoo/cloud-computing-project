#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file

# beam-playground:
#   name: WordCount
#   description: An example that counts words in Shakespeare's works.
#   multifile: false
#   pipeline_options: --output output.txt
#   context_line: 87
#   categories:
#     - Combiners
#     - Options
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - options
#     - count
#     - combine
#     - strings

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
    sum_x_pos = 0.0
    sum_x_neg = 0.0
    sum_y_pos = 0.0
    sum_y_neg = 0.0
    count_x_pos = 0
    count_x_neg = 0
    count_y_pos = 0
    count_y_neg = 0
    accumulator = sum_x_pos, sum_x_neg, sum_y_pos, sum_y_neg, count_x_pos, count_x_neg, count_y_pos, count_y_neg
    return accumulator

  def add_input(self, accumulator, input):
    sum_x_pos, sum_x_neg, sum_y_pos, sum_y_neg, count_x_pos, count_x_neg, count_y_pos, count_y_neg = accumulator
    xVal = float(input[0])
    yVal = float(input[1])
    if xVal > 3:
       sum_x_pos += xVal
       count_x_pos += 1
    elif xVal < -3:
       sum_x_neg += xVal
       count_x_neg += 1
    if yVal > 3:
       sum_y_pos += yVal
       count_y_pos += 1
    elif yVal < -3:
       sum_y_neg += xVal
       count_y_neg += 1
    return sum_x_pos, sum_x_neg, sum_y_pos, sum_y_neg, count_x_pos, count_x_neg, count_y_pos, count_y_neg

  def merge_accumulators(self, accumulators):
    sum_x_pos, sum_x_neg, sum_y_pos, sum_y_neg, count_x_pos, count_x_neg, count_y_pos, count_y_neg = zip(*accumulators)
    return sum(sum_x_pos), sum(sum_x_neg), sum(sum_y_pos), sum(sum_y_neg), sum(count_x_pos), sum(count_x_neg), sum(count_y_pos), sum(count_y_neg)

  def extract_output(self, accumulator):
    sum_x_pos, sum_x_neg, sum_y_pos, sum_y_neg, count_x_pos, count_x_neg, count_y_pos, count_y_neg = accumulator
    result_x_pos = 0
    result_x_neg = 0
    result_y_pos = 0
    result_y_neg = 0
    if count_x_pos > 0:
      result_x_pos = round(sum_x_pos / count_x_pos, 2)
    if count_x_neg > 0:
      result_x_neg = round(sum_x_neg / count_x_neg, 2)
    if count_y_pos > 0:
      result_y_pos = round(sum_y_pos / count_y_pos, 2)
    if count_y_neg > 0:
      result_y_neg = round(sum_y_neg / count_y_neg, 2)
    return (result_x_pos, result_x_neg, result_y_pos, result_y_neg)

class ToDictionaryDoFn(beam.DoFn):
  def process(self, element):
    result = {}
    result['second'] = element[0]
    result['avg_x_velocity_pos'] = element[1][0]
    result['avg_x_velocity_neg'] = element[1][1]
    result['avg_y_velocity_pos'] = element[1][2]
    result['avg_y_velocity_neg'] = element[1][3]
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
        data = p | 'ReadFromBQ' >> beam.io.gcp.bigquery.ReadFromBigQuery(table=known_args.input)

        formatData = data | 'Format Data' >> beam.Map(lambda record: (record['frame']//25, (record['xVelocity'], record['yVelocity'])))

        calcVelocities = formatData | 'Calculate Average Velocities' >> beam.CombinePerKey(VelocityAverageFn())

        toDict = calcVelocities | 'To Dictionary' >> beam.ParDo(ToDictionaryDoFn())

        schema = 'second:INTEGER, avg_x_velocity_pos:FLOAT, avg_x_velocity_neg:FLOAT, avg_y_velocity_pos:FLOAT, avg_y_velocity_neg:FLOAT'

        toDict | 'WriteToBQ' >> beam.io.gcp.bigquery.WriteToBigQuery(
            known_args.output,
            schema=schema,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
