from dm_helper import GaFlattenerDeploymentConfiguration
def generate_config(ctx):

    config = GaFlattenerDeploymentConfiguration(ctx.env)

    resources = {
      'resources': [{
          'name': 'sink-name',
          'type': 'gcp-types/logging-v2:projects.sinks',
          'properties': {
              'sink': '{sink_name}'.format(sink_name=config.get_sink_name()),
              'destination': 'pubsub.googleapis.com/projects/{gcp_project}/topics/{topic_name}'.format(
                    gcp_project=config.get_project(),topic_name=config.get_topic_name()
                    ),
              'filter': config.get_filter(),
              'outputVersionFormat': 'V2'
            }
       }]
    }
    return resources