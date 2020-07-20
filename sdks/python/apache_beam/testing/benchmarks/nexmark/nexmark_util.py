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

"""Utilities for the Nexmark suite.

The Nexmark suite is a series of queries (streaming pipelines) performed
on a simulation of auction events. This util includes:

  - A Command class used to terminate the streaming jobs
    launched in nexmark_launcher.py by the DirectRunner.
  - A ParseEventFn DoFn to parse events received from PubSub.

Usage:

To run a process for a certain duration, define in the code:
  command = Command(process_to_terminate, args)
  command.run(timeout=duration)

"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import logging
import threading

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.testing.benchmarks.nexmark.models import nexmark_model

_LOGGER = logging.getLogger(__name__)

METRICS_NAMESPACE = 'nexmark'


class SourceType(object):
  """Enum-like object represating possible sources for events.

  TODO(BEAM-7372): Convert this into enum once we stop supporting Python 2.7
  """
  PUBSUB = 'PUBSUB'
  KAFKA = 'KAFKA'


class Command(object):
  def __init__(self, cmd, args):
    self.cmd = cmd
    self.args = args

  def run(self, timeout):
    def thread_target():
      logging.debug(
          'Starting thread for %d seconds: %s', timeout, self.cmd.__name__)

      self.cmd(*self.args)
      _LOGGER.info(
          '%d seconds elapsed. Thread (%s) finished.',
          timeout,
          self.cmd.__name__)

    thread = threading.Thread(target=thread_target, name='Thread-timeout')
    thread.daemon = True
    thread.start()
    thread.join(timeout)


class ParseEventFn(beam.DoFn):
  """Parses the raw event info into a Python objects.

  Each event line has the following format:

    person: 'p',id',name,email,credit_card,city, \
                          state,timestamp,extra
    auction: 'a',id,item_name,description,initial_bid, \
                          reserve_price,timestamp,expires,seller,category,extra
    bid: 'b',id,bidder,price,timestamp,extra

  For example:

    'p,12345,maria,maria@maria.com,1234-5678-9012-3456, \
                                        sunnyvale,CA,1528098831536'
    'a,12345,car67,2012 hyundai elantra,15000,20000, \
                                        1528098831536,20180630,maria,vehicle'
    'b,12345,maria,20000,1528098831536'
  """
  def __init__(self):
    self.messages_counter = Metrics.counter(METRICS_NAMESPACE, 'events')
    self.model_dict = {
        'p': nexmark_model.Person,
        'a': nexmark_model.Auction,
        'b': nexmark_model.Bid,
    }

  def process(self, elem):
    self.messages_counter.inc()

    if isinstance(elem, bytes):
      elem = elem.decode()

    row = elem.split(',')
    model = self.model_dict.get(row.pop(0))
    if not model:
      raise ValueError('Invalid event: %s.' % row)

    event = model(*row)
    logging.debug('Parsed event: %s', event)
    yield event


def display(elm):
  logging.debug(elm)
  return elm
