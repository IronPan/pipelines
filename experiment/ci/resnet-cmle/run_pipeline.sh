#!/bin/bash
#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 {the "License"};
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

# This script automated the process to release the component images.
# To run it, find a good release candidate commit SHA from ml-pipeline-staging project,
# and provide a full github COMMIT SHA to the script. E.g.
# ./release.sh 2118baf752d3d30a8e43141165e13573b20d85b8
# The script copies the images from staging to prod, and update the local code.
# You can then send a PR using your local branch.

STAGING_BASE_DIR=$1
echo "${STAGING_BASE_DIR}"
gsutil cp -r ./preprocess "${STAGING_BASE_DIR}/preprocess"

pushd ./train
python setup.py sdist
ls
cp ./dist/*.tar.gz ./dist/train.tar.gz
gsutil cp ./dist/train.tar.gz "${STAGING_BASE_DIR}/train/train.tar.gz"
popd

python resnet-train-pipeline.py --package_base_dir "${STAGING_BASE_DIR}"
gsutil cp ./resnet-train-pipeline.py.zip "${STAGING_BASE_DIR}/resnet-train-pipeline.zip"

kfp run submit -e resnet-train-pipeline -f ./resnet-train-pipeline.py.zip -w ${@:2}