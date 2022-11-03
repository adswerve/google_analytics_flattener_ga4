from dm_helper import GaFlattenerDeploymentConfiguration


def generate_config(ctx):
    config = GaFlattenerDeploymentConfiguration(ctx.env)

    resources = {
        'resources': [{
            'name': 'metric-name',
            'type': 'gcp-types/logging-v2:projects.metrics',
            'properties': {
                'metric': config.get_sink_name(),
                'filter': config.get_filter(),
            }
        }]
    }
    return resources
