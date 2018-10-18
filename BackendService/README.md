
# Backend Service
Steps to run the code:
1. export PROJECT="Project Name"
2. export BUCKET="Bucket Name"
3. python create_template.py - this will create a Dataflow Job template and store it the bucket specified
4. gcloud app deploy - this deploys the backend service to the app engine
5. gcloud app deploy cron.yaml - this deploys the cron schedule for the backend service
