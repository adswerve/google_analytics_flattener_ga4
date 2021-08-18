from dm_helper import GaFlattenerDeploymentConfiguration


def generate_config(ctx):
    config = GaFlattenerDeploymentConfiguration(ctx.env)

    resources = {
        'resources': [{
            'name': config.get_bucket_name(),
            'type': 'gcp-types/storage-v1:buckets',
            'properties': {
                'predefinedAcl': 'projectPrivate',
                'projection': 'full',
                'location': 'US',
                'storageClass': 'STANDARD'
            }
        }]
    }
    return resources
