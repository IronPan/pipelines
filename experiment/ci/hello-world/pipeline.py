#!/usr/bin/env python3
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import kfp.dsl as dsl
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--commit_id', help='Commit Id', type=str)
args = parser.parse_args()

@dsl.pipeline(
    name='Hello World CI pipeline',
    description='Basic sample to show how to build docker image as part of KFP CI'
)
def helloworld_ci_pipeline():
  dsl.ContainerOp(
      name='Print Hello World',
      image='gcr.io/ml-pipeline-dogfood/helloworld-ci:' + args.commit_id,
  )


if __name__ == '__main__':
  import kfp.compiler as compiler
  compiler.Compiler().compile(helloworld_ci_pipeline, __file__ + '.zip')