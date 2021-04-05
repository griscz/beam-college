export BEAM_VERSION="2.28.0"
export GCP_PROJECT="your-gcp-project"
export GCP_REGION="us-central1"
export GCS_BUCKET="your-gcp-bucket"
export IMAGE="gcr.io/${GCP_PROJECT}/custom/beam_python3.7_sdk:${BEAM_VERSION}-custom"

########
# SETUP
# I have already run this on my computer. 
# This is just to give you an idea of what I did.
# This section assumes I have already installed:
# * Google Cloud SDK (gcloud CLI),
# * pyenv
# * docker
# * libgmp libmpfr libmpc (for custom package)
########
# GCP-specific (Dataflow, GCR)
gcloud config set project $GCP_PROJECT
gcloud auth login
gcloud auth configure-docker

## Python environment
pyenv install 3.7.9
pyenv virtualenv 3.7.9 beam-python37
pyenv activate beam-python37

#######
# PYTHON PACKAGES for running Beam pipeline
#######
pip install apache-beam==$BEAM_VERSION
pip install apache-beam[gcp]

# Custom demo package - may need to preinstall: libgmp libmpfr libmpc
pip install sparkles/

######
# Build Image
######
docker build --no-cache \
			       --build-arg beam_version=$BEAM_VERSION \
		         -t $IMAGE \
		         .
docker push $IMAGE

## You can launch a container using your image to make sure the contents are as expected:
docker run -it --entrypoint="/bin/bash" $IMAGE

######
# Running Pipelines
######
# Testing locally - NOTE: Local paths for input/output refer to the
# *container filesystem*, not your local filesystem; these will
# disappear when container finishes running.
python demo_pipeline.py \
--input=kinglear.txt \
--output /tmp/counts \
--runner=PortableRunner \
--job_endpoint=embed \
# CUSTOM CONTAINER FLAGS:
# Caches the container
--environment_type="DOCKER" \
--environment_config="${IMAGE}"

# # Example - Flink Runner
python demo_pipeline.py \
--input=kinglear.txt \
--output /tmp/counts \
--runner=FlinkRunner \
# CUSTOM CONTAINER FLAGS:
# Caches the container
--environment_cache_millis=10000 \
--environment_type="DOCKER" \
--environment_config="${IMAGE}"

# Example - Dataflow
python demo_pipeline.py \
  --job_name "beam-customcontainer-demo" \
  --input gs://dataflow-samples/shakespeare/kinglear.txt \
  --output "gs://${GCS_BUCKET}/counts" \
  --temp_location "gs://${GCS_BUCKET}/tmp/" \
  --runner DataflowRunner \
  --project $(gcloud config get-value project) \
  --region $GCP_REGION\
  # CUSTOM CONTAINER FLAGS:
  --experiment=use_runner_v2 \
  --worker_harness_container_image=$IMAGE_URL
