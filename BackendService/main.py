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
The app for the 'frontend' service, which handles cron job requests to
fetch tweets and store them in the Datastore.
"""

import datetime
import logging
import os
import httplib2
import sys

from googleapiclient import discovery

from google.appengine.ext import ndb
import twitter
#import tweepy
import webapp2
from googleapiclient import discovery
import httplib2
from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build


class tweet(ndb.Model):
    """Define the Tweet model."""
    userName = ndb.StringProperty()
    Actualtweet = ndb.StringProperty()
    City = ndb.StringProperty()
    created_at = ndb.DateTimeProperty()
    tid = ndb.IntegerProperty()
    urls = ndb.StringProperty(repeated=True)
    Hashtags = ndb.StringProperty(repeated=True)
    country = ndb.StringProperty()


class finalProcessedTweets(ndb.Model):
    """Define the Tweet model."""
    Tweet = ndb.StringProperty()
    Job_List = ndb.StringProperty(repeated=True)
    Company_Name = ndb.StringProperty()
    Location = ndb.StringProperty()
    Job_Url = ndb.StringProperty(repeated=True)
    created_at = ndb.DateTimeProperty()

class processedTweets(ndb.Model):
    """Define the Tweet model."""
    Tweet = ndb.StringProperty()
    Job_List = ndb.StringProperty(repeated=True)
    Company_Name = ndb.StringProperty()
    Location = ndb.StringProperty()
    Job_Url = ndb.StringProperty(repeated=True)
    created_at = ndb.DateTimeProperty()


class ProcessEntities(webapp2.RequestHandler):
    def get_service(self):
        #credentials = GoogleCredentials.get_application_default().create_scoped([
        #'https://www.googleapis.com/auth/cloud-platform'])
        http = httplib2.Http(timeout=60)
        #credentials.authorize(http)
        return discovery.build('language', 'v1beta1', http=http,
        developerKey='AIzaSyCgBAk-811OZnyBU86ErCFcFUqfcPb6Dg8')

    def annotate_text(self, text, encoding='UTF32',
                  extract_syntax=False, extract_entities=False,
                  extract_document_sentiment=False):
        body = {
            'document': {
                'type': 'PLAIN_TEXT',
                'content': text,
            },
            'features': {
                'extract_syntax': extract_syntax,
                'extract_entities': extract_entities,
                'extract_document_sentiment': extract_document_sentiment,
            },
            'encoding_type': encoding,
        }
        service = self.get_service()
        request = service.documents().annotateText(body=body)
        response = request.execute()
        return response

    def get(self):
        # These env vars are set in app.yaml.
        PROJECT = os.environ['PROJECT']
        BUCKET = os.environ['BUCKET']
        TEMPLATE = os.environ['TEMPLATE_NAME']
        logging.info("Starting with processing the tweets for entities")
        timeStamp = datetime.datetime.utcnow() - datetime.timedelta(hours = 4)
        tweet_entities = ndb.gql('select * from processedTweets ORDER BY created_at DESC')
        logging.info("Queried Tweet Table. Starting with processing Entities")
        for te in tweet_entities:
            #logging.info("Processing Tweet ", dir(te))
            created_at = te.created_at
            '''
            if created_at < timeStamp:
                break
            '''
            nlp_org, nlp_loc = "", ""
            entities = self.annotate_text(te.Tweet, extract_entities = True)
            if entities.get('error', None) is not None:
                logging.warning("Error getting entities: %s", entities['error'])
            else:
                for entity in entities['entities']:
                    if entity['type'] == 'ORGANIZATION':
                        nlp_org = entity['name']
                    elif entity['type'] == 'LOCATION':
                        nlp_loc = entity['name']
            tw = finalProcessedTweets()
            # logging.info("text: %s, %s", tweet.text, tweet.user.screen_name)
            tw.Tweet = te.Tweet
            tw.Job_List = te.Job_List
            tw.Company_Name = nlp_org if nlp_org != "" else te.Company_Name
            tw.Location = nlp_loc if te.Location is None else te.Location
            tw.Job_Url = te.Job_Url
            tw.created_at = te.created_at
            tw.put()

        logging.info("Entity Processing completed")
        self.response.write('Done')

class LaunchJob(webapp2.RequestHandler):
  """Launch the Dataflow pipeline using a job template."""

  def get(self):
    is_cron = self.request.headers.get('X-Appengine-Cron', False)
    # logging.info("is_cron is %s", is_cron)
    # Comment out the following check to allow non-cron-initiated requests.
    if not is_cron:
      return 'Blocked.'
    # These env vars are set in app.yaml.
    PROJECT = os.environ['PROJECT']
    BUCKET = os.environ['BUCKET']
    TEMPLATE = os.environ['TEMPLATE_NAME']

    # Because we're using the same job name each time, if you try to launch one
    # job while another is still running, the second will fail.
    JOBNAME = PROJECT + '-twproc-template'

    credentials = GoogleCredentials.get_application_default()
    service = build('dataflow', 'v1b3', credentials=credentials)
    currentTimeStamp = str(datetime.datetime.utcnow())
    BODY = {
            "jobName": "{jobname}".format(jobname=JOBNAME),
            "gcsPath": "gs://{bucket}/templates/{template}".format(
                bucket=BUCKET, template=TEMPLATE),
            "parameters": {"timestamp": currentTimeStamp},
             "environment": {
                "tempLocation": "gs://{bucket}/temp".format(bucket=BUCKET),
                "zone": "us-central1-f"
             }
        }

    dfrequest = service.projects().templates().create(
        projectId=PROJECT, body=BODY)
    dfresponse = dfrequest.execute()
    logging.info(dfresponse)
    self.response.write('Done')

class FetchTweets(webapp2.RequestHandler):
  """Fetch home timeline tweets from the given twitter account."""

  def get(self):

    # set up the twitter client. These env vars are set in app.yaml.
    consumer_key = os.environ['CONSUMER_KEY']
    consumer_secret = os.environ['CONSUMER_SECRET']
    access_token = os.environ['ACCESS_TOKEN']
    access_token_secret = os.environ['ACCESS_TOKEN_SECRET']

    api = twitter.Api(consumer_key=consumer_key,
                      consumer_secret=consumer_secret,
                      access_token_key=access_token,
                      access_token_secret=access_token_secret)

    last_id = None
    public_tweets = None

    # see if we can get the id of the most recent tweet stored.
    tweet_entities = ndb.gql('select * from tweet order by tid desc limit 1')
    for te in tweet_entities:
      last_id = te.tid
      break
    if last_id:
      logging.info("last id is: %s", last_id)

    query ='q=San%20Francisco%20%23hiring%20OR%20%23resume%20OR%20%23JobSearch%20OR%20%23Hiring%20OR%20%23Careers%20OR%20%23NowHiring'
    #import pdb
    #pdb.set_trace()
    searched_tweets = []
    max_tweets = 100
    while len(searched_tweets) < max_tweets:
        count = max_tweets - len(searched_tweets)
        try:

            #new_tweets = api.search(q=query, count=count, max_id=str(last_id - 1)
            #                        ,include_entities = True, tweet_mode='extended')
            if last_id is not None:
                new_tweets = api.GetSearch(raw_query=query+"%20&result_type=recent&count=100", since_id=last_id)
            else:
                new_tweets = api.GetSearch(raw_query=query+"%20&result_type=recent&count=100")
            if not new_tweets:
                break
            searched_tweets.extend(new_tweets)
            last_id = new_tweets[-1].id
        except Exception as e:
            # depending on TweepError.code, one may want to retry or wait
            # to keep things simple, we will give up on an error
             logging.warning("Error getting tweets: %s", e)
             break
    # store the retrieved tweets in the datastore
    logging.info("got %s tweets", len(searched_tweets))
    for twee in searched_tweets:
      tw = tweet()
      # logging.info("text: %s, %s", tweet.text, tweet.user.screen_name)
      tw.Actualtweet = twee.text
      tw.user = twee.user.screen_name
      tw.created_at = datetime.datetime.strptime(
          twee.created_at, "%a %b %d %H:%M:%S +0000 %Y")
      tw.tid = twee.id
      tw.urls = map(lambda x: x['expanded_url'], twee._json['entities']['urls'])
      tw.Hashtags = map(lambda x: x['text'], twee._json['entities']['hashtags'])
      country, place = "", ""
      if twee._json.get('place', None) is not None:
          country = twee._json['place'].get('country', None)
          place = twee._json['place'].get('name', None)
      tw.country = country
      tw.City = place
      tw.key = ndb.Key(tweet, twee.id)
      tw.put()

    self.response.write('Done')


class MainPage(webapp2.RequestHandler):
  def get(self):
    self.response.write('nothing to see.')


app = webapp2.WSGIApplication(
    [('/', MainPage), ('/timeline', FetchTweets),
     ('/launchtemplatejob', LaunchJob),
     ('/ProcessEntities', ProcessEntities)],
    debug=True)
