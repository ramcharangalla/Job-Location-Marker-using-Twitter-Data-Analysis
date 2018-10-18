#!/usr/local/bin/python
# coding: utf-8

import httplib2
import sys

from googleapiclient import discovery
from googleapiclient.errors import HttpError

def test_language_api(content):
    discovery_url = 'https://{api}.googleapis.com/$discovery/rest?version={apiVersion}'
    service = discovery.build('language', 'v1', http=httplib2.Http(),
    discoveryServiceUrl=discovery_url,
    developerKey='AIzaSyCgBAk-811OZnyBU86ErCFcFUqfcPb6Dg8')
    service_request = service.documents().annotateText( body={
            'document': {
                'type': 'PLAIN_TEXT',
                'content': content
                },
                'features': {
                    'extractEntities': True
                },
                'encodingType': 'UTF16' if sys.maxunicode == 65535 else 'UTF32'
                })
    try:
        response = service_request.execute()
    except HttpError as e:
        response = {'error': e}
    return response

sample_content = 'RT @CaPOST_Jobs: New Police Lieutenant #job w/ UC San Francisco @UCSF @UCSF_Police #LEjobsCA #Hiring #jobs https://t.co/QCjpj4CLfd'
print test_language_api(sample_content)
