# Steps to configure e2e
- create a .cloudbuild.yaml
- add cloud build service account as IAP user
- parameterize the gcs code path in pipeline.py 
- add following steps to cloud build yaml
   - upload source code to gcs
   - compile pipeline.py to tarball and upload to gcs
   - push the pipeline package to KFP

