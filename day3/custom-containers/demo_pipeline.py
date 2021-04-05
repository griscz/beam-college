from __future__ import absolute_import
from past.builtins import unicode

import argparse
import logging
import os
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.iobase import Write
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from sparkles import sparkles 
from sparkles.sink import Utf8TextSink 

class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
    return re.findall(r'[\w\']+', element, re.UNICODE)

def format_output(w, c):
  # USING A CUSTOM LIBRARY
  output = sparkles.add_sparkles(w, c)
  logging.info('OUTPUT %s', output)
  return output

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:
    # Read the text file.
    lines = p | 'Read' >> ReadFromText(known_args.input)

    output = (
      lines
      | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
      | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
      | 'GroupAndSum' >> beam.CombinePerKey(sum)
      # For Logging Purposes
      | 'Format' >> beam.MapTuple(format_output))

    # A custom text sink so it displays nicely in GCS :(
    output | 'Write' >> Write(Utf8TextSink(known_args.output))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
