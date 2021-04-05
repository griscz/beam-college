# Session: Custom Containers

This folder contains the demo code for the Custom Containers session for Beam College.

The main script is [`demo-script.sh`](demo-script.sh). Remember to change the variables as needed.

It covers:
* Building a custom container image from a [`Dockerfile`](Dockerfile)
* Launching a Beam pipeline with this custom SDK harness image using [`demo_pipeline.py`](demo_pipeline.py)

## Prerequisites

To launch the pipeline and build the image, you will need:

* pyenv, Python3, pip
* Docker
* Google Cloud SDK (for Google Cloud Dataflow/Container Registry)

We will also be installing the PYPI package for apache-beam and some custom/third-party packages.

## License

Apache 2.0; see [`LICENSE`](LICENSE) for details.

## Contact

If you have further questions about this feature or demo, you can email user@beam.apache.org or 
[contact us](https://beam.apache.org/community/contact-us/) through other Apache Beam community channels.
