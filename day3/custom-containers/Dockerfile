ARG beam_version=2.28.0

# Base image is Apache Beam Python 3.7 SDK Harness
FROM apache/beam_python3.7_sdk:${beam_version}

# Set an environment variable for sparkles sink.
ENV DEMO_HEADER="Header"

# Add custom package files (could also be a py wheel)
ADD /sparkles /opt/sparkles

# This is specifically for local testing -
# with Dataflow we'll be using a remote source.
COPY kinglear.txt kinglear.txt

# Install dependencies of gmpy2
RUN apt-get update && \
	apt-get install -y libgmp-dev libmpfr-dev libmpc-dev

# Install preloaded package
RUN pip install /opt/sparkles
