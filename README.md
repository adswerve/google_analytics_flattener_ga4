# README #

Temporary repo for Open Source GA Flattener tool.  
Directories:
* cf : cloud function files
* tools : utilities to help with testing and backfilling data
* tests : units test

# PREREQUISITES #
1. Add "Logs Configuration Writer", "Cloud Functions Developer" pre
   defined IAM roles to
   [PROJECT_NUMBER]@cloudservices.gserviceaccount.com (built in service
   account) otherwise deployment will fail with permission errors. See
   <https://cloud.google.com/deployment-manager/docs/access-control> for
   detailed explanation.
2. Install gCloud
3. Clone github open source repo
4. Create Google GCP project
5. Enable the Cloud Build API 
6. Enable the Cloud Functions API
7. Create bucket for staging code during deployment (apmk-staging,
   as-dev-gord-staging, aka: <project_name>-staging. TODO: automatic
   this

# Commands #
* gcloud config set project analyticspros.com:spotted-cinnamon-834 
* gcloud config set account username@domain.com
* gcloud deployment-manager deployments create deployment-ga-flattene
  --config ga_flattener.yaml
* gcloud deployment-manager deployments delete deployment-ga-flattener
  -q
