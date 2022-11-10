from dm_helper import GaFlattenerDeploymentConfiguration


def generate_config(ctx):
    config = GaFlattenerDeploymentConfiguration(ctx.env)

    resources = {
        'resources': [{
            'name': 'sink-name',
            'type': 'gcp-types/logging-v2:projects.sinks',
            'properties': {
                'sink': config.get_sink_name(),
                'destination': f"pubsub.googleapis.com/projects/{config.get_project()}/topics/{config.get_topic_id()}",
                'filter': config.get_filter(),
                'outputVersionFormat': 'V2'
            }
       },
          {
              'name': 'sink-name-intraday',
              'type': 'gcp-types/logging-v2:projects.sinks',
              'properties': {
                  'sink': f"{config.get_sink_name(intraday=True)}",
                  'destination': f"pubsub.googleapis.com/projects/{config.get_project()}/topics/{config.get_topic_id(intraday=True)}",
                  'filter': config.get_filter(intraday=True),
                  'outputVersionFormat': 'V2'
              }
          }
      ]
    }
    return resources
