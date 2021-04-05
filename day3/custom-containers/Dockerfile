ARG beam_version
ARG keyfile

# Base image is Apache Beam Python 3.7 SDK Harness
FROM apache/beam_python3.7_sdk:${beam_version}

# Set an environment variable for sparkles sink.
ENV DEMO_HEADER="Env Var Output Header"

# Add files
COPY kinglear.txt kinglear.txt
ADD /sparkles /opt/sparkles

# Install dependencies of gmpy2
RUN apt-get update && \
	apt-get install -y libgmp-dev libmpfr-dev libmpc-dev

# Install preloaded package
RUN pip install /opt/sparkles
