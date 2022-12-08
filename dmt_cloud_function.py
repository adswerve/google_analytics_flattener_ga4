# Copyright 2017 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Cloud Function (nicely deployed in deployment) DM template."""

import base64
import hashlib
from io import BytesIO
import zipfile
from dm_helper import GaFlattenerDeploymentConfiguration


def generate_config(ctx):
  config = GaFlattenerDeploymentConfiguration(ctx.env)
  """Generate YAML resource configuration."""
  in_memory_output_file = BytesIO()
  function_name = config.get_cf_name(code_location=ctx.properties['codeLocation'])
  zip_file = zipfile.ZipFile(
      in_memory_output_file, mode='w', compression=zipfile.ZIP_DEFLATED)
  for imp in ctx.imports:
    if imp.startswith(ctx.properties['codeLocation']):
      zip_file.writestr(imp[len(ctx.properties['codeLocation']):],
                        ctx.imports[imp])
  # used the files below to extract a copy of what parameter 'ctx' contains for unit testing.
  # Would be nice to get the object seriallize but I was unnsuccessfull in doing so.  TODO - nice to have
  # zip_file.writestr("ctx_imports.json",json.dumps(ctx.imports))
  # zip_file.writestr("ctx_env.json", json.dumps(ctx.env))
  # zip_file.writestr("ctx_properties.json", json.dumps(ctx.properties))
  zip_file.close()
  content = base64.b64encode(in_memory_output_file.getvalue())
  m = hashlib.md5()
  m.update(content)
  source_archive_url = f"gs://{ctx.properties['codeBucket']}/{m.hexdigest()}.zip"

  chunk_length = 3500
  content_chunks = [content[ii:ii + chunk_length] for ii in range(0, len(content), chunk_length)]
  # use `>` first in case the file exists
  cmds = [f"echo '{content_chunks[0].decode('ascii')}' | base64 -d > /function/function.zip;"]
  # then use `>>` to append
  cmds += [
      f"echo '{chunk.decode('ascii')}' | base64 -d >> /function/function.zip;"
      for chunk in content_chunks[1:]
  ]

  volumes = [{'name': 'function-code', 'path': '/function'}]

  zip_steps = [
      {
          'name': 'ubuntu',
          'args': ['bash', '-c', cmd],
          'volumes': volumes,
      } for cmd in cmds
  ]
  build_step = {
      'name': f"upload-{function_name}-code",
      'action': 'gcp-types/cloudbuild-v1:cloudbuild.projects.builds.create',
      'metadata': {
          'runtimePolicy': ['UPDATE_ON_CHANGE']
      },
      'properties': {
          'steps': zip_steps + [{
              'name': 'gcr.io/cloud-builders/gsutil',
              'args': ['cp', '/function/function.zip', source_archive_url],
              'volumes': volumes
          }],
          'timeout':
              '120s'
      }
  }
  cloud_function = {
      'type': 'gcp-types/cloudfunctions-v1:projects.locations.functions',
      'name': function_name,
      'properties': {
          'parent':
              '/'.join([
                  'projects', config.get_project(), 'locations',
                  ctx.properties['location']
              ]),
          'function':
              function_name,
          'labels': {
              # Add the hash of the contents to trigger an update if the bucket
              # object changes
              'content-md5': m.hexdigest()
          },
          'sourceArchiveUrl':
              source_archive_url,
          'environmentVariables': {
              'codeHash': m.hexdigest()
          },
          'entryPoint':
              ctx.properties['entryPoint'],
          'timeout':
              ctx.properties['timeout'],
          'availableMemoryMb':
              ctx.properties['availableMemoryMb'],
          'runtime':
              ctx.properties['runtime']
      },
      'metadata': {
          'dependsOn': [f"upload-{function_name}-code"]
      }
  }

  if ctx.properties['triggerType'] == 'http':
      cloud_function['properties']['httpsTrigger']= {}
  elif ctx.properties['triggerType'] == 'pubsub':
      if ctx.properties['codeLocation'] in ["cfintraday/", "cfintradaysqlview/"]:
          cloud_function['properties']['eventTrigger'] = {
              'resource': f"projects/{config.get_project()}/topics/{config.get_topic_id(intraday=True)}{'config' if function_name.__contains__('config') else ''}",
              'eventType': 'providers/cloud.pubsub/eventTypes/topic.publish'}
      else:
          cloud_function['properties']['eventTrigger'] = {
                  'resource': f"projects/{config.get_project()}/topics/{config.get_topic_id()}{'config' if function_name.__contains__('config') else ''}",
                  'eventType': 'providers/cloud.pubsub/eventTypes/topic.publish'}
  elif ctx.properties['triggerType'] == 'gcs':
      cloud_function['properties']['eventTrigger'] = {
          'resource': f"projects/{config.get_project()}/buckets/{config.get_bucket_name()}",
          'eventType': 'google.storage.object.finalize'}


  #add user environment variables to cloud function
  for key, value in config.user_environment_variables.items():
      cloud_function["properties"]['environmentVariables'][key] = value

  resources = [build_step, cloud_function]

  return {
      'resources':
          resources,
      'outputs': [{
          'name': 'sourceArchiveUrl',
          'value': source_archive_url
      }, {
          'name': 'name',
          'value': '$(ref.' + function_name + '.name)'
      }]
  }
