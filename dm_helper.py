class DeploymentConfiguration(object):
    def __init__(self, context_environment_vars):
        '''
        From
        https://cloud.google.com/deployment-manager/docs/configuration/templates/use-environment-variables

        Keys allowed for built in environment variables related to deployment using deployment manager

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
        resource.type="bigquery_resource" 
        protoPayload.methodName="jobservice.jobcompleted" 
        protoPayload.serviceData.jobCompletedEvent.eventName="load_job_completed" 
        protoPayload.authenticationInfo.principalEmail="firebase-measurement@system.gserviceaccount.com" 
        NOT protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.load.destinationTable.tableId:"events_intraday"
        '''
        self.FILTER_INTRADAY = '''
        resource.type="bigquery_resource" 
        protoPayload.methodName="tableservice.insert" OR protoPayload.methodName="tableservice.delete"
        "events_intraday_"
        '''
        self.user_environment_variables = {
            "CONFIG_BUCKET_NAME": self.get_bucket_name(),
            "CONFIG_FILENAME": "config_datasets.json",
            "EVENTS": "events",
            "EVENT_PARAMS": "event_params",
            "USER_PROPERTIES": "user_properties",
            "ITEMS": "items",
            "LOCATION_ID": "us-central1",
            "TOPIC_NAME": self.get_topic_name()
        }

    def get_topic_name(self, intraday=False):
        '''
        TODO: make sure returned value meets resource name requirements defined in GCP
        '''
        if intraday:
            return '{d}-topic-intraday'.format(d=self._createValidGCPResourceName(self.deployment))
        else:
            return '{d}-topic'.format(d=self._createValidGCPResourceName(self.deployment))

    def get_sink_name(self, intraday=False):
        '''
        TODO: make sure returned value meets resource name requirements defined in GCP
        '''
        if intraday:
            return '{d}-sink-intraday'.format(d=self._createValidGCPResourceName(self.deployment)
                                     , n=self._createValidGCPResourceName(self.name))
        else:
            return '{d}-sink'.format(d=self._createValidGCPResourceName(self.deployment)
                                 , n=self._createValidGCPResourceName(self.name))

    def get_project(self):
        return self.deployment_gcp_project

    def get_project_number(self):
        return self.deployment_gcp_project_number

    def get_bucket_name(self):
        return '{d}-{n}-adswerve-ga-flat-config'.format(d=self._create_valid_gcp_resource_name(self.deployment)
                                                        , n=self._create_valid_gcp_resource_name(
                self.get_project_number()))[:62]

    def get_filter(self, intraday=False):
        #TODO: add feature for 3 options:
        #       1. Daily tables only
        #       2. Intra day tables only
        #       3. Both Intra and Daily tables
        if intraday:
            return self.FILTER_INTRADAY
        else:
            return self.FILTER

    def _create_valid_gcp_resource_name(self, p_field):
        '''
        GCP resources must only contain letters, numbers, undrescores, dots or dashes
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
