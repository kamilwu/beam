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

"""Nexmark launcher.

The Nexmark suite is a series of queries (streaming pipelines) performed
on a simulation of auction events. The launcher orchestrates the generation
and parsing of streaming events and the running of queries.

Model
  - Person: Author of an auction or a bid.
  - Auction: Item under auction.
  - Bid: A bid for an item under auction.

Events
 - Create Person
 - Create Auction
 - Create Bid

Queries
  - Query0: Pass through (send and receive auction events).

Usage
  - DirectRunner
      python nexmark_launcher.py \
          --query/q <query number> \
          --source_type [KAFKA|PUBSUB] \
          --number_of_events <number of events> \
          --topic_name <topic name> \
          --project <project id> \
          --loglevel=DEBUG (optional) \
          --wait_until_finish_duration <time_in_ms> \
          --streaming

  - DataflowRunner
      python nexmark_launcher.py \
          --query/q <query number> \
          --source_type [KAFKA|PUBSUB] \
          --number_of_events <number of events> \
          --topic_name <topic name> \
          --project <project id> \
          --region <GCE region> \
          --loglevel=DEBUG (optional) \
          --wait_until_finish_duration <time_in_ms> \
          --streaming \
          --sdk_location <apache_beam tar.gz> \
          --staging_location=gs://... \
          --temp_location=gs://

"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import argparse
import logging
import time
import sys
import uuid

from google.cloud import pubsub

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TestOptions
from apache_beam.metrics import MetricsFilter
from apache_beam.testing.benchmarks.nexmark.nexmark_util import Command
from apache_beam.testing.benchmarks.nexmark.nexmark_util import SourceType
from apache_beam.testing.benchmarks.nexmark.nexmark_util import METRICS_NAMESPACE
from apache_beam.testing.benchmarks.nexmark.queries import query0
from apache_beam.testing.benchmarks.nexmark.queries import query1
from apache_beam.testing.benchmarks.nexmark.queries import query2
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsReader


class NexmarkLauncher(object):
  def __init__(self):
    self.parse_args()

  def parse_args(self):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--query',
        '-q',
        type=int,
        action='append',
        required=True,
        choices=[0, 1, 2],
        help='Query to run')

    parser.add_argument(
        '--subscription_name',
        type=str,
        help='Pub/Sub subscription to read from.')

    parser.add_argument(
        '--topic_name',
        type=str,
        required=True,
        help='Pub/Sub or Kafka topic to read from.')

    parser.add_argument(
        '--loglevel',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set logging level to debug.')

    parser.add_argument(
        '--source_type',
        type=str,
        choices=[SourceType.KAFKA, SourceType.PUBSUB],
        required=True,
        help='Source for events.')

    parser.add_argument(
        '--bootstrap_server', type=str, help='Kafka Bootstrap Server domain.')

    parser.add_argument(
        '--number_of_events',
        type=int,
        required=True,
        help='Number of events to be processed.')

    args, self.pipeline_args = parser.parse_known_args()
    logging.basicConfig(
        level=getattr(logging, args.loglevel, None),
        format='(%(threadName)-10s) %(message)s')

    self.pipeline_options = PipelineOptions(self.pipeline_args)
    logging.debug('args, pipeline_args: %s, %s', args, self.pipeline_args)

    # Usage with Dataflow requires a project to be supplied.
    self.project = self.pipeline_options.view_as(GoogleCloudOptions).project

    # Pub/Sub and KafkaIO are currently available for use only in streaming
    # pipelines.
    streaming = self.pipeline_options.view_as(StandardOptions).streaming
    if streaming is None:
      parser.print_usage()
      print(sys.argv[0] + ': error: argument --streaming is required')
      sys.exit(1)

    self.runner = self.pipeline_options.view_as(StandardOptions).runner

    # wait_until_finish ensures that the streaming job is canceled.
    wait_until_finish_duration = self.pipeline_options.view_as(
        TestOptions).wait_until_finish_duration
    if wait_until_finish_duration is None:
      parser.print_usage()
      print(
          sys.argv[0] +
          ': error: argument --wait_until_finish_duration is required')
      sys.exit(1)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    self.pipeline_options.view_as(SetupOptions).save_main_session = True

    self.topic_name = args.topic_name
    self.subscription_name = args.subscription_name
    self.bootstrap_server = args.bootstrap_server
    self.source_type = args.source_type
    self.queries = args.query
    self.number_of_events = args.number_of_events

  def get_source(self, pipeline):
    if self.source_type == SourceType.PUBSUB:
      topic_path = self.get_pubsub_topic()

      if self.subscription_name:
        sub_path = self.get_pubsub_subscription()

        logging.debug('Using subscription %s' % sub_path)
        raw_events = pipeline | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
            subscription=sub_path)
      else:
        logging.debug('Using topic %s' % topic_path)
        raw_events = pipeline | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
            topic=topic_path)
    elif self.source_type == SourceType.KAFKA:
      logging.debug('Using Kafka topic %s' % self.topic_name)
      # Setting max_num_records while using Dataflow triggers an exception:
      # KeyError: 'beam:window_fn:serialized_java:v1'
      max_num_records = None if self.runner == 'DataflowRunner' else \
          self.number_of_events
      raw_events = (
          pipeline
          | 'ReadFromKafka' >> ReadFromKafka(
              consumer_config={
                  'bootstrap.servers': self.bootstrap_server,
                  'auto.offset.reset': 'earliest',
                  'group.id': uuid.uuid4().hex,
              },
              topics=[self.topic_name],
              max_num_records=max_num_records)
          | 'Extract values' >> beam.Values())
    else:
      raise RuntimeError('Unsupported source type')

    return raw_events

  def get_pubsub_topic(self):
    pub_client = pubsub.PublisherClient()
    topic_path = pub_client.topic_path(self.project, self.topic_name)
    return topic_path

  def get_pubsub_subscription(self):
    sub_client = pubsub.SubscriberClient()
    sub_path = sub_client.subscription_path(
        self.project, self.subscription_name)
    return sub_path

  def run_query(self, query, query_args, query_errors):
    try:
      pipeline = beam.Pipeline(options=PipelineOptions(self.pipeline_args))
      events = query.load(self.get_source(pipeline), query_args)
      self.apply_monitor(events)
      result = pipeline.run()

      job_duration = self.pipeline_options.view_as(
          TestOptions).wait_until_finish_duration
      # Dataflow needs special handling to cancel streaming jobs.
      # We are querying metrics every 10 seconds to see how many events
      # have been processed so far.
      if self.runner == 'DataflowRunner':
        self.wait_for_streaming_job_termination(result, job_duration)
      else:
        result.wait_until_finish(duration=job_duration)

      self.show_metrics(result)
    except Exception as exc:
      query_errors.append(str(exc))
      raise

  @staticmethod
  def apply_monitor(pipeline):
    return pipeline | 'Measure time' >> beam.ParDo(
        MeasureTime(namespace=METRICS_NAMESPACE))

  def wait_for_streaming_job_termination(
      self, result, job_duration, retry_interval=10):
    """Terminates a streaming job once the condition is met.

    The runner must provide support for querying metrics during pipeline
    execution for this to work properly.
    """
    start_time = time.time()
    elapsed = 0
    job_duration_sec = job_duration // 1000
    logging.info('Waiting for streaming job to terminate.')

    while elapsed < job_duration_sec:
      filters = MetricsFilter().with_namespace(METRICS_NAMESPACE).with_name(
          'events')
      metrics = result.metrics().query(filters)
      try:
        condition_met = metrics['counters'][0].result >= self.number_of_events
      except IndexError:
        condition_met = False

      if condition_met:
        result.cancel()
        return
      time.sleep(retry_interval)
      elapsed = time.time() - start_time
    result.cancel()
    raise RuntimeError(
        'Timing out on waiting for job after %d seconds' % job_duration_sec)

  def show_metrics(self, result):
    filters = MetricsFilter().with_namespace(METRICS_NAMESPACE)
    reader = MetricsReader(namespace=METRICS_NAMESPACE, filters=filters)
    reader.publish_metrics(result)

  def run(self):
    queries = {
        0: query0,
        1: query1,
        2: query2,  # TODO(mariagh): Add more queries.
    }

    # TODO(mariagh): Move to a config file.
    query_args = {2: {'auction_id': 'a1003'}}

    query_errors = []
    for i in self.queries:
      logging.info('Running query %d', i)

      # The DirectRunner is the default runner, and it needs
      # special handling to cancel streaming jobs.
      launch_from_direct_runner = self.runner in [None, 'DirectRunner']

      query_duration = self.pipeline_options.view_as(
          TestOptions).wait_until_finish_duration
      if launch_from_direct_runner:
        command = Command(
            self.run_query, args=[queries[i], query_args.get(i), query_errors])
        command.run(timeout=query_duration // 1000)
      else:
        self.run_query(queries[i], query_args.get(i), query_errors)

    if query_errors:
      logging.error('Query failed with %s', ', '.join(query_errors))
    else:
      logging.info('Queries run: %s', self.queries)


if __name__ == '__main__':
  launcher = NexmarkLauncher()
  launcher.run()
