class DeploymentConfiguration(object):
    def __init__(self, context_environment_vars):
        '''
        From
        https://cloud.google.com/deployment-manager/docs/configuration/templates/use-environment-variables

        Keys allowed for built-in environment variables related to deployment using deployment manager

        deployment:	The name of the deployment.
        name:	The name declared in the configuration that is using the template. This can be useful if you want the
                name you declare in the configuration to be the name of the resource in the underlying templates.
        project: The project ID for this deployment.
        project_number: The project number for this deployment.
        current_time:   The UTC timestamp when expansion started for the deployment.
        type:	The resource type declared in the top-level configuration.
        username:	The current Deployment Manager user.

        For example:
        >>gcloud config list
        [core]
        account = first.last@domain.com
        disable_usage_reporting = False
        project = some-project
        [gcloudignore]
        enabled = true
        Your active configuration is: [default]

        >>gcloud deployment-manager deployments create deploy-cmdline-name --config dm.yaml

        would result in:

        "deployment": "deploy-cmdline-name"
        "project": "some-project"
        "current_time": 1592015636
        "project_number": "423452468050"
        "username": "first.last@domain.com"
        NOTE:  Dependent on which template calls generate_config()
        "name": "function"
        "type": "dmt_cloud_function.py"

        '''
        self.deployment = context_environment_vars['deployment']
        self.name = context_environment_vars['name']
        self.deployment_gcp_project = context_environment_vars['project']
        self.deployment_gcp_project_number = context_environment_vars['project_number']
        self.deployment_utc_time = context_environment_vars['current_time']
        self.type = context_environment_vars['type']
        self.username = context_environment_vars['username']


class GaFlattenerDeploymentConfiguration(DeploymentConfiguration):
    def __init__(self, context_environment_vars):
        super(GaFlattenerDeploymentConfiguration, self).__init__(context_environment_vars)
        self.FILTER = '''
        resource.type="bigquery_dataset" 
        protoPayload.methodName="google.cloud.bigquery.v2.JobService.InsertJob" 
        protoPayload.authenticationInfo.principalEmail="firebase-measurement@system.gserviceaccount.com" 
        severity: "NOTICE"
        NOT "events_intraday_"
        '''
        self.FILTER_INTRADAY = '''
        resource.type="bigquery_resource" 
        protoPayload.methodName="tableservice.insert" OR protoPayload.methodName="tableservice.delete"
        "events_intraday_"
        protoPayload.authenticationInfo.principalEmail="firebase-measurement@system.gserviceaccount.com"
        NOT severity="error"
        '''
        # we exclude severity="error" in intraday log filter, because we only want successful creations of intraday table
        # there are logs about unsuccessful creation of this table: "table already exists" -
        # they cause the intraday CF to fail:
        # File "/workspace/main.py", line 160, in manage_intraday_schedule
        #     input_event = InputValidatorIntraday(event)
        #   File "/workspace/main.py", line 22, in __init__
        #     self.gcp_project = bq_destination_table['projectId']
        # KeyError: 'projectId'
        # this is because the log about unsuccessful creation of the intraday nested table is not the right log for us
        # it doesn't have table information in the API response part

        self.gcp_resource_name_limit = 63
        """As of 2021-11-09, there are the following chars limits:
            Deployment name         63
            CF name                 63
            GCS bucket              63
            Pub / Sub topic id      255
            Log sink name           100
            
           Deployment name will determine the names/ids of GCP resources.
           This constant will help truncate deployment name, so the length of GCP resources is valid,
           to prevent installation errors 
            """

        self.user_environment_variables = {
            "CONFIG_BUCKET_NAME": self.get_bucket_name(),
            "CONFIG_FILENAME": "config_datasets.json",
            "EVENTS": "events",
            "EVENT_PARAMS": "event_params",
            "USER_PROPERTIES": "user_properties",
            "ITEMS": "items",
            "LOCATION_ID": "us-central1",
            "TOPIC_NAME": self.get_topic_id()
        }

    def get_project(self):
        return self.deployment_gcp_project

    def get_project_number(self):
        return self.deployment_gcp_project_number

    def get_filter(self, intraday=False):
        if intraday:
            return self.FILTER_INTRADAY
        else:
            return self.FILTER

    def get_sink_name(self, intraday=False):
        '''
        As long as deployment name is valid, sink name will also be valid
        As of 2021-11-09:
            Deployment name can be up to 63 chars long.
            Sink name can be up to 100 chars long.
        '''
        if intraday:
            return f"{self._create_valid_gcp_resource_name(self.deployment)}-sink-intraday"
        else:
            return f"{self._create_valid_gcp_resource_name(self.deployment)}-sink"

    def get_topic_id(self, intraday=False):
        '''
        Returns topic id
        As long as deployment name is valid, topic id will also be valid
        As of 2021-11-09:
            Deployment name can be up to 63 chars long.
            Pub/Sub topic id can be up to 255 chars long.
        '''
        if intraday:
            return f"{self._create_valid_gcp_resource_name(self.deployment)}-topic-intraday"
        else:
            return f"{self._create_valid_gcp_resource_name(self.deployment)}-topic"

    def get_cf_name(self, code_location):
        """
        As of 2021-11-09:
            Deployment name can be up to 63 chars long.
            CF name can also be up to 63 chars long.

        We will be appending a string to deployment name to get a bucket name.
        We need to truncate deployment name, so installation doesn't fail.
        """
        fn = code_location[:-1]

        # -1 because of delimiter hyphen
        trunc_deployment_name_length = self.gcp_resource_name_limit - len(fn) - 1

        return f"{self.deployment[:trunc_deployment_name_length]}-{fn}"

    def get_bucket_name(self):
        """
        As of 2021-11-09:
            Deployment name can be up to 63 chars long.
            GCS bucket name can also be up to 63 chars long.

        We will be appending a string to deployment name to get a bucket name.
        We need to truncate deployment name, so installation doesn't fail.
        """
        suffix = "adswerve-ga-flat-config"
        number = self._create_valid_gcp_resource_name(self.get_project_number())

        # -2 because we have 2 additional hyphens which are delimiters between deployment, name and
        trunc_deployment_name_length = self.gcp_resource_name_limit - len(suffix) - len(number) - 2

        deployment = self._create_valid_gcp_resource_name(self.deployment)[:trunc_deployment_name_length]

        return f"{deployment}-{number}-{suffix}"

    def _create_valid_gcp_resource_name(self, p_field):
        '''
        GCP resources must only contain letters, numbers, underscores, dots or dashes
        and be between 3 and 63 chars long.  Resources must start with a letter and may not end with a dash
        :param p_field: starting point of the field
        :return: cleaned big query field name
        '''
        r = ""
        for char in p_field.lower():
            if char.isalnum():
                r += char
            else:
                r += "-"
        return r
