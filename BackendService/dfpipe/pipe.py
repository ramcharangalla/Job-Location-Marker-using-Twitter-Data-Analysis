# Copyright 2017 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Define and launch a Dataflow pipeline to analyze recent tweets stored
in the Datastore.
"""

from __future__ import absolute_import

import datetime
import json
import logging
import re
import os
import uuid
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
from apache_beam import combiners
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from apache_beam.pvalue import AsDict
from apache_beam.pvalue import AsSingleton
from apache_beam.options.pipeline_options import PipelineOptions


from google.cloud.proto.datastore.v1 import query_pb2
from google.cloud.proto.datastore.v1 import entity_pb2
from googledatastore import helper as datastore_helper, PropertyFilter

location_Regex = re.compile(r'(san\s?francisco|san\s?jose|oakland|berkley|palo\s+alto|mountain\s?view|cupertino|fremont|sunnyvale|sacramento)', re.IGNORECASE)
job_Regex = re.compile(r'(sales|developer|Full\s?Stack\s?Engineer|Associate|Teacher|Chef|Dev\s?Ops|Marketing|Recruiter|Media|Sales|Developer|Product\s?Manager|Director|Software\s?Engineer|Vice\s?President/)', re.IGNORECASE)
default_location = 'San Francisco'
logging.basicConfig(level=logging.INFO)

class FilterDate(beam.DoFn):
  """Filter Tweet datastore entities based on timestamp."""

  def __init__(self, opts, hours):
    super(FilterDate, self).__init__()
    self.opts = opts
    self.hours = hours
    self.earlier = None

  def start_bundle(self):
    before = datetime.datetime.strptime(self.opts.timestamp.get(),
        '%Y-%m-%d %H:%M:%S.%f')
    self.earlier = before - datetime.timedelta(hours=self.hours)

  def process(self, element):
    created_at = element.properties.get('created_at', None)
    cav = None
    if created_at:
      cav = created_at.timestamp_value
      cseconds = cav.seconds
    else:
      return
    crdt = datetime.datetime.fromtimestamp(cseconds)
    logging.warn("crdt: %s", crdt)
    logging.warn("earlier: %s", self.earlier)
    if crdt > self.earlier:
      # return only the elements (datastore entities) with a 'created_at' date
      # within the last self.days days.
      yield element

class processTweet(beam.DoFn):

  """
    Process each tweet and generate the following fields
    1. tweet - String containing the tweet
    2. Job Field - List of Job categories
    3. Company Name - Name of the firm
    4. Location - Name of the city
  """
  def __init__(self, user_options):
      self.opts = user_options

  def process(self, element):
    """Returns an iterator over words in contents of Cloud Datastore entity.
    The element is a line of text.  If the line is blank, note that, too.
    Args:
      element: the input element to be processed
    Returns:
      The processed element.
    """
    processTime = datetime.datetime.strptime(self.opts.timestamp.get(),
        '%Y-%m-%d %H:%M:%S.%f')
    tweet = element.properties.get('Actualtweet', None)
    tweet_text = ''
    if tweet:
      tweet_text = tweet.string_value
    orgName = element.properties.get('userName', None)
    orgName = "" if orgName is None else orgName.string_value
    # We fill figure it out while doing entity extraction
    location = element.properties.get('City', None)
    location = location.string_value if location is not None else None
    if not tweet_text:
      jobs = []
      urls = ""
    else:
        jobs = re.findall(job_Regex, tweet_text)
        if location is None or location.strip() == "":
            location = location_Regex.findall(tweet_text)
            location = location[0] if len(location) > 0 else default_location

    url_content = element.properties.get('urls', None)
    links = []
    if url_content:
      urls = url_content.array_value.values
      for u in urls:
        links.append(u.string_value.lower())


    logging.warn("processed tweet:", jobs, location)
    yield {
        "Tweet":tweet_text,
        "Job_List": ', '.join(jobs),
        "Company_Name": orgName,
        "Location": location,
        "Job_Url": ', '.join(links),
        "created_at": processTime
    }

class WordExtractingDoFn(beam.DoFn):
  """Parse each tweet text into words, removing some 'stopwords'."""

  def process(self, element):
    content_value = element.properties.get('text', None)
    text_line = ''
    if content_value:
      text_line = content_value.string_value

    words = set([x.lower() for x in re.findall(r'[A-Za-z\']+', text_line)])
    # You can add more stopwords if you want.  These words are not included
    # in the analysis.
    stopwords = [
        'a', 'amp', 'an', 'and', 'are', 'as', 'at', 'be', 'been',
        'but', 'by', 'co', 'do', 'for', 'has', 'have', 'he', 'her', 'his',
        'https', 'if', 'in', 'is', 'it', 'me', 'my', 'no', 'not', 'of', 'on',
        'or', 'rt', 's', 'she', 'so', 't', 'than', 'that', 'the', 'they',
        'this', 'to', 'us', 'was', 'we', 'what', 'with', 'you', 'your'
        'who', 'when', 'via']
    stopwords += list(map(chr, range(97, 123)))
    return list(words - set(stopwords))


class CoOccurExtractingDoFn(beam.DoFn):
  """Parse each tweet text into words, and after removing some 'stopwords',
  emit the bigrams.
  """

  def process(self, element):
    content_value = element.properties.get('text', None)
    text_line = ''
    if content_value:
      text_line = content_value.string_value

    words = set([x.lower() for x in re.findall(r'[A-Za-z\']+', text_line)])
    # You can add more stopwords if you want.  These words are not included
    # in the analysis.
    stopwords = [
        'a', 'amp', 'an', 'and', 'are', 'as', 'at', 'be', 'been',
        'but', 'by', 'co', 'do', 'for', 'has', 'have', 'he', 'her', 'his',
        'https', 'if', 'in', 'is', 'it', 'me', 'my', 'no', 'not', 'of', 'on',
        'or', 'rt', 's', 'she', 'so', 't', 'than', 'that', 'the', 'they',
        'this', 'to', 'us', 'was', 'we', 'what', 'with', 'you', 'your',
        'who', 'when', 'via']
    stopwords += list(map(chr, range(97, 123)))
    pruned_words = list(words - set(stopwords))
    pruned_words.sort()
    import itertools
    return list(itertools.combinations(pruned_words, 2))


class URLExtractingDoFn(beam.DoFn):
  """Extract the urls from each tweet."""

  def process(self, element):
    url_content = element.properties.get('urls', None)
    if url_content:
      urls = url_content.array_value.values
      links = []
      for u in urls:
        links.append(u.string_value.lower())
      return links


class QueryDatastore(beam.PTransform):
  """Generate a Datastore query, then read from the Datastore.
  """

  def __init__(self, project, hours):
    super(QueryDatastore, self).__init__()
    self.project = project
    self.hours = hours


  # it's not currently supported to use template runtime value providers for
  # the Datastore input source, so we can't use runtime values to
  # construct our query. However, we can still statically filter based on time
  # of template construction, which lets us make the query a bit more
  # efficient.
  def expand(self, pcoll):
    query = query_pb2.Query()
    query.kind.add().name = 'tweet'
    now = datetime.datetime.now()
    # The 'earlier' var will be set to a static value on template creation.
    # That is, because of the way that templates work, the value is defined
    # at template compile time, not runtime.
    # But defining a filter based on this value will still serve to make the
    # query more efficient than if we didn't filter at all.
    earlier = now - datetime.timedelta(hours=self.hours)
    '''
    datastore_helper.set_property_filter(query.filter, 'created_at',
                                         PropertyFilter.GREATER_THAN,
                                         earlier)
    '''
    return (pcoll
            | 'read from datastore' >> ReadFromDatastore(self.project,
                                                         query, None,
                                                         num_splits = 1))
class EntityWrapper(object):
  """Create a Cloud Datastore entity from the given string."""
  def __init__(self, namespace, kind, ancestor):
    self._namespace = namespace
    self._kind = kind
    self._ancestor = ancestor

  def make_entity(self, content):
    entity = entity_pb2.Entity()
    if self._namespace is not None:
      entity.key.partition_id.namespace_id = self._namespace

    # All entities created will have the same ancestor
    datastore_helper.add_key_path(entity.key, self._kind, self._ancestor,
                                  self._kind, str(uuid.uuid4()))

    datastore_helper.add_properties(entity, content)
    return entity

class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      parser.add_value_provider_argument('--timestamp', type=str)


def process_datastore_tweets(project, pipeline_options):
  """Creates a pipeline that reads tweets from Cloud Datastore from the last
  N days. The pipeline finds the top most-used words, the top most-tweeted
  URLs, ranks word co-occurrences by an 'interestingness' metric (similar to
  on tf* idf).
  """

  user_options = pipeline_options.view_as(UserOptions)
  hours = 20
  p = beam.Pipeline(options=pipeline_options)

  # Read entities from Cloud Datastore into a PCollection, then filter to get
  # only the entities from the last DAYS days.
  lines = (p | QueryDatastore(project, hours)
             | beam.ParDo(FilterDate(user_options, hours))
      )

  # Process the tweet.
  processedTweets = (lines
      | 'processTweets' >> (beam.ParDo(processTweet(user_options))))

  # Define some inline helper functions.

  def join_cinfo(cooccur, percents):
    """Calculate a co-occurence ranking."""
    import math

    word1 = cooccur[0][0]
    word2 = cooccur[0][1]
    try:
      word1_percent = percents[word1]
      weight1 = 1 / word1_percent
      word2_percent = percents[word2]
      weight2 = 1 / word2_percent
      return (cooccur[0], cooccur[1], cooccur[1] *
              math.log(min(weight1, weight2)))
    except:
      return 0

  def generate_cooccur_schema():
    """BigQuery schema for the word co-occurrence table."""
    json_str = json.dumps({'fields': [
          {'name': 'w1', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'name': 'w2', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'name': 'count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
          {'name': 'log_weight', 'type': 'FLOAT', 'mode': 'NULLABLE'},
          {'name': 'ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}]})
          # {'name': 'ts', 'type': 'STRING', 'mode': 'NULLABLE'}]})
    return parse_table_schema_from_json(json_str)

  def generate_url_schema():
    """BigQuery schema for the urls count table."""
    json_str = json.dumps({'fields': [
          {'name': 'url', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'name': 'count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
          {'name': 'ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}]})
          # {'name': 'ts', 'type': 'STRING', 'mode': 'NULLABLE'}]})
    return parse_table_schema_from_json(json_str)

  def generate_wc_schema():
    """BigQuery schema for the word count table."""
    json_str = json.dumps({'fields': [
          {'name': 'word', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'name': 'percent', 'type': 'FLOAT', 'mode': 'NULLABLE'},
          {'name': 'ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}]})
          # {'name': 'ts', 'type': 'STRING', 'mode': 'NULLABLE'}]})
    return parse_table_schema_from_json(json_str)

  # Write the results to three BigQuery tables.
  (processedTweets
   | 'create entity' >> beam.Map(EntityWrapper("",
                                               "processedTweets",
                                               "root").make_entity)
   | 'processed tweet write' >> WriteToDatastore(project))

  # Actually run the pipeline.
  return p.run()

if __name__ == '__main__':
    PROJECT = os.environ['PROJECT']
    BUCKET = os.environ['BUCKET']

    pipeline_options = {
        'project': PROJECT,
        'staging_location': 'gs://' + BUCKET + '/staging',
        'runner': 'direct',
        'setup_file': './setup.py',
        'job_name': PROJECT + '-twcount',
        'temp_location': 'gs://' + BUCKET + '/temp',
        'template_location': 'gs://' + BUCKET + '/templates/' + PROJECT + '-twproc_tmpl'
    }
    # define and launch the pipeline (non-blocking), which will create the template.
    pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
    process_datastore_tweets(PROJECT, pipeline_options)
