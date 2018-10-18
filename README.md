# Job-Location-Marker-using-Twitter-Data-Analysis

A Full stack application for users to search for job postings locations on Maps by analyzing recent Tweets in the area.
Developed using GCP Dataflow pipeline for timed extraction of raw Data from Twitter into a GCP Datastore and another for running
MapReduce analysis and aggregation on the Data to derive job specifics like location, Tweet and Organization.


A lot of companies post on Twitter about job openings at one of their locations as well as about
upcoming recruitment drives. So for our project, we created a system to capture such tweets and figure
out the location of the job posting or recruitment drive, the company name and shows them on a map.
This can enable people looking for jobs to easily look for openings near them. We provided a web-based
map interface to the users for searching for jobs nearby, we used Google DataFlow, DataStore, Cloud
Storage, Cloud NLP and App Engine for processing, storing and serving the relevant data.

## Architecture

Backend Service
The backend service is responsible for fetching data from twitter processing it and making it
available for the frontend service to use. Following are the three functionalities that the backend service
performs
1) Crawling Twitter: The first operation that the service performs is fetching data from
Twitter, the operation is done in near real time i.e every 17 minutes, this is done so as to
circumvent Twitter’s rate limitation restrictions.
2) Parsing the Tweets: Once we have the tweets from twitter we extract multiple
information from the text of the tweet namely - the job posting urls, City name. For this
we run a mapreduce job using Google’s Cloud DataFlow to extract this information. The
information extraction happens by matching regex patterns to the text of the tweet
3) Entity (Organization name) recognition: As we wanted to not only show the tweet and
city but rather also the name of the company that is posting the job we needed a way to
figure out the name of the firm from the text of the tweet. We couldn’t have used the
name of the author of the tweet cause many a times the tweet is posted by an HR
personnel or by some other 3rd party staffing service. For recognizing the company
names and entities in general in tweets we used Google’s Cloud NLP. Apart from filling
in the company name we also use the entity service to fill in the missing city locations.


Frontend Service
The frontend service will be fetching the processed tweets from cloud datastore and display
them to the user. The details are displayed to user in boxes which would have the information of
Company Name, Tweet, Location and when the box is clicked it would redirect the user to the job
posting. All the cities would be displayed on a map with the total number of jobs at that place to the
users.
