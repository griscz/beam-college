author: Ilya Kozyrev
summary: In this codelab you will add a new input source to an Apache Beam pipeline using Beam Schema API, create Dataflow template from the Apache Beam pipeline and run the Dataflow tempalte in Google Cloud.
id: data-flow-templates-new-source-schema-api
categories: Dataflow,Apache Beam, GCP,Dataflow template,Dataflow Flex template
environments: Web
status: Draft
feedback link:https://github.com/ilya-kozyrev/codelabs


# Add new input source to the Dataflow template using Apache Beam Schema API
<!-- ------------------------ -->
## Introduction
Duration: 0:05:00

#### Goals

In this codelab you will add a new input source to an Apache Beam pipeline using Beam Schema API, create Dataflow template from the Apache Beam pipeline and run the Dataflow template in Google Cloud. This colab demonstrates how to work with Beam Schema API and how Beam Schema API helps to support multiple data formats in your pipeline.

#### What you'll build

* New transformation for .parquet format into the Beam Row abstraction
* New transformation from Beam Row into .parquet format
* Dataflow Flex template

#### What you'll learn

* How to use Apache Beam Schema API
* How to work with Apache Beam ParquetIO
* How to build Dataflow Flex template from pipeline
* How to run Dataflow template in Google Cloud

#### What you'll need

* A computer with a modern web browser installed (Chrome is recommended)
* Your favourite code editor or IDE
* A Google Cloud project

Next step: environment setup

<!-- ------------------------ -->
## Environment setup
Duration: 0:05:00

#### Install Java Environment

In order to work with Beam Java SDK you need *Java 1.8*, *mvn* (the java command line tool for build) installed.

1) Download and install the [Java Development Kit (JDK)](https://www.oracle.com/technetwork/java/javase/downloads/index.html) version 8. Verify that the [JAVA_HOME](https://docs.oracle.com/javase/8/docs/technotes/guides/troubleshoot/envvars001.html) environment variable is set and points to your JDK installation.

2) Download and install [Apache Maven](https://maven.apache.org/download.cgi) by following Maven’s [installation guide](https://maven.apache.org/install.html) for your specific operating system.

#### Install Cloud SDK
Download and install [Cloud SDK](https://cloud.google.com/sdk) by following Google's SDK [quickstart guide](https://cloud.google.com/sdk/docs/quickstart)


#### Install Git
Instructions below are provided for macOS, and you can find instructions for other environments [here](https://git-scm.com/download/)

```bash
$ brew install git
```

#### Clone the colab template repository
```bash
$ git clone https://github.com/akvelon/DataflowTemplates.git
```

#### Switch to the working branch
```bash
$ git checkout ProtegrityIntegrationTemplate
```

<!-- ------------------------ -->
## Project overview
Duration: 0:05:00

The cloned repository contains `src` and `v2` folders. `src` and `v2` folders contain [Classic and Flex templates](https://cloud.google.com/dataflow/docs/concepts/dataflow-templates) correspondingly. In this codelab you will work with Dataflow Flex template. 

Navigate to `DataflowTemplates/v2/protegrity-data-tokenization` folder that contains the template packages.

![image_caption](resources/Screenshot-2021-03-23-at-16-39-21.png)


Packages structure is:
* `options` package contains all pipeline parameters and logic around them
* `templates` package contains the template main class that creates the pipeline and applies all transformations
* `transforms` package contains custom Beam transforms that are applied to the pipeline
    * `io` subpackage contains transforms for input/output, e.g. to convert from format to Row and vise versa.
  
* `utils` package contains utils supporting pipeline operations, such us schema parsing or working wiht CSV
* `resources` folder contains template metadata file.

Next is overview of the template main class.
<!-- ------------------------ -->
## Template main class
Duration: 0:15:00

Navigate to the main template class `ProtegrityDataTokenization` in your editor. This class locates in
`templates/ProtegrityDataTokenization.java`

`ProtegrityDataTokenization` class you may see `main` method parses options and calls `run` method
```java 
  public static void main(String[] args) {
    ProtegrityDataTokenizationOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(ProtegrityDataTokenizationOptions.class);
    FileSystems.setDefaultPipelineOptions(options);

    DataflowPipelineOptions dataflowOptions = PipelineOptionsFactory.fromArgs(args)
        .withoutStrictParsing()
        .withValidation()
        .as(DataflowPipelineOptions.class);

    run(options, dataflowOptions);
  }
```


`run` method implements Apache Beam pipeline flow: 
1) Pipeline creation:
```java 
Pipeline pipeline = Pipeline.create(options);
```
2) Applying necessary transformations:
```java
pipeline.apply()
```
3) Pipeline running by calling `run` method.
```java
pipeline.run();
```

#### Data Schema
Presented pipeline reads input data schema in BigQuery compatible format to build Beam Rows
```java 
schema = new SchemasUtils(options.getDataSchemaGcsPath(), StandardCharsets.UTF_8);
```

#### Coder
Beam provides coder mechanism for serialisation/deserialization process. 
And for operate with some types you need specify coders by ```CoderRegistry``` or by 
applying ```.setCoder()``` to the PCollection. And for this case used Beam Schema API to represent 
data as Rows in PCollection.
```java
CoderRegistry coderRegistry = pipeline.getCoderRegistry();
coderRegistry
    .registerCoderForType(RowCoder.of(schema.getBeamSchema()).getEncodedTypeDescriptor(), 
        RowCoder.of(schema.getBeamSchema()));
```

Next step: data schema in BigQuery compatible format.
<!-- ------------------------ -->
## Data schema
Duration: 0:05:00

Beam schema requires a type mapping mechanism from the input format to Beam Row. 
The type mapping mechanism depends on the input format. Structured formats data schemas examples:
* autogenerated, e.g., BigQuery datasets with tables metadata that enables to retrieve schema, autogenerated schema for Avro input formats 
* user-provided, e.g., structured formats like JSON and CSV where user need to specify the schema for converting into Beam Rows. 

One of the best approaches for the user-provided data schema is to use BigQuery Schema in JSON format and parse it into Beam Schema

```json
{
  "fields": [
    {
      "name": "ssn",
      "type": "STRING",
      "mode": "REQUIRED"
    },
    {
      "name": "firstname",
      "type": "STRING",
      "mode": "REQUIRED"
    },
    {
      "name": "lastname",
      "type": "STRING",
      "mode": "NULLABLE"
    }
  ]
}
```
Create JSON with `fields` collection that includes all input fields as JSON objects.

Object should contain three attributes:
* `name` name of the field
* `type` type of the field, in BigQuery [types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types?hl=pl). All Beam types supported by BigQuery
* `mode` can be `REQUIRED` or `NULLABLE`

#### Schema parsing

To parse schema implement parsing from JSON string using `BigQueryHelpers.fromJsonString()`

```java
void parseJson(String jsonSchema) throws UnsupportedOperationException {
    TableSchema schema = BigQueryHelpers.fromJsonString(jsonSchema, TableSchema.class);
    validateSchemaTypes(schema);
    bigQuerySchema = schema;
    jsonBeamSchema = BigQueryHelpers.toJsonString(schema.getFields());
}
```

And then convert ```TableSchema``` to Beam `Schema` using `org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.fromTableSchema`

You may see how it implemented in presented pipeline `v2/protegrity-data-tokenization/src/main/java/com/google/cloud/teleport/v2/utils/SchemasUtils.java` class

Nex step: IO transform overview
<!-- ------------------------ -->
## GcsIO overview
Duration: 0:05:00

To transform many input formats to the Beam Row and from Beam Row into destination format, presented
pipeline implements IO transforms. Let's focus on GcsIO that works with Google Cloud Storage.

In the `/transforms/io/GcsIO.java` find `FORMAT` enum, that lists all supported formats.
```java
public enum FORMAT {
    JSON,
    CSV,
    AVRO
}
```

For IO functionality `GcsIO` class provides methods `read` 
```java
public PCollection<Row> read(Pipeline pipeline, SchemasUtils schema) {
    switch (options.getInputGcsFileFormat()) {
      case CSV:
        ...
      case JSON:
        return readJson(pipeline)
            .apply(new JsonToBeamRow(options.getNonTokenizedDeadLetterGcsPath(), schema));
      case AVRO:
        return readAvro(pipeline, schema.getBeamSchema());
      default:
        throw new IllegalStateException(
            "No valid format for input data is provided. Please, choose JSON or CSV.");

    }
  }
```

and `write`
```java
public POutput write(PCollection<Row> input, Schema schema) {
    switch (options.getOutputGcsFileFormat()) {
      case JSON:
        return writeJson(input);
      case AVRO:
        return writeAvro(input, schema);
      case CSV:
        return writeCsv(input, schema.getFieldNames());
      default:
        throw new IllegalStateException(
            "No valid format for output data is provided. Please, choose JSON or CSV.");

    }
}
```

For each supported format GcsIO encapsulates transformations from source format to Beam Row and 
from Beam Row to destination format for writing results. e.g. `readAvro` or `writeAvro`. 
These methods take pipeline and Schema objects as arguments.

Next step: implement parquet files reading, and writing and transformation into the Beam Row.
<!-- ------------------------ -->
## Parquet IO read
Duration: 0:05:00

To implement new parquet source read and write, first add the new `FORMAT` enum value

```java
public enum FORMAT {
    JSON,
    CSV,
    AVRO,
    PARQUET
  }
```

Next, create a method to read parquet from the filesystem and convert it into Beam Row.

To read parquet use ParquetIO from Beam SDK. Apply `ParquetIO.read()` to the pipeline object.
```java
PCollection<GenericRecord> parquetRecords =
        pipeline.apply(ParquetIO.read(avroSchema).from(options.getInputGcsFilePattern()));
```

`ParquetIO.read()` takes Avro schema as an argument. Also you need to convert Beam Schema to the Avro
schema using `AvroUtils.toAvroSchema()`
```
org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);
```

Applying `ParquetIO.read()` to the pipeline produces `PCollection` of `GenericRecord` and it should 
be converted into the Beam Row. A simple way to do that is by applying `MapElements.into` to the 
`PCollection<GenericRecord>`. Parameter that you need to provide to `via` method is transform 
function from `GenericRecord` into `Row`. And luckily it is also available in `AvroUtils`.

```java
parquetRecords
    .apply("GenericRecordsToRow", MapElements.into(TypeDescriptor.of(Row.class))
        .via(AvroUtils.getGenericRecordToRowFunction(beamSchema)))
```

Finally, specify the coder for the new PCollection. 
A fully implemented method: 


```
private PCollection<Row> readParquet(Pipeline pipeline, Schema beamSchema) {
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    PCollection<GenericRecord> parquetRecords =
        pipeline.apply(ParquetIO.read(avroSchema).from(options.getInputGcsFilePattern()));

    return parquetRecords
        .apply("GenericRecordsToRow", MapElements.into(TypeDescriptor.of(Row.class))
            .via(AvroUtils.getGenericRecordToRowFunction(beamSchema)))
        .setCoder(RowCoder.of(beamSchema));
 }
```

Next step: implement write method for writing Beam Row to the parquet format.
<!-- ------------------------ -->

## Parquet IO write
Duration: 0:05:00

Let's implement write method for parquet format.

You need to create a method to transform Beam Row to parquet and write files to filesystem.

To write parquet you might use `FileIO` with `ParquetIO.sink()` from Beam SDK. 
You need to apply `FileIO.<GenericRecord>write()` to the `PCollection<GenericRecord>`.

First step is to convert `PCollection<Row>` into `PCollection<GenericRecord>`. 
It's very similar to the implementation of conversion from GenericRecord to Row. For backward
conversion you might use `MapElements` with `AvroUtils.getRowToGenericRecordFunction()`
```java
PCollection<GenericRecord> genericRecords = outputCollection
     .apply(
        "RowToGenericRecord", MapElements.into(TypeDescriptor.of(GenericRecord.class))
            .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
```

As you notice, `getRowToGenericRecordFunction` takes Avro Schema. Beam Schema can be converted to
the Avro schema using `AvroUtils`

```
org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);
```

Now that you have prepared `PCollection<GenericRecord>` next step is to apply `FileIO.write()` that requires Generic type. Pass `avroSchema` as the argument to `ParquetIO.sink()`. Destination path, you may take from the pipeline options.

```java
genericRecords.apply(FileIO.<GenericRecord>write()
    .via(ParquetIO.sink(avroSchema))
    .to(options.getOutputGcsDirectory()).withSuffix(".parquet")
```

Next step: tokenization service

<!-- ------------------------ -->
## Tokenization service
Duration: 0:05:00

Presented templates provides data tokenization using an external REST service 
and configured to communicate with the tokenization service in following request format: 
```json
{
  ...
  "data":[
    {"<key>":"<value>", ...},
    {"<key>":"<value>", ...},
    {"<key>":"<value>", ...},
    ...
  ]
  ...
}
```
If your tokenization service have similar requirements for request structure, just skip next codelab 
section.

#### If you don't have tokenization service
To skip the tokenization step for testing purposes replace the tokenization call in 
`ProtegrityDataProtectors`

1. Naviagate to `ProtegrityDataProtectors.java`
2. Find processBufferedRows method
3. Replace

```java
private void processBufferedRows(Iterable<Row> rows, WindowedContext context) {

    try {
      for (Row outputRow : getTokenizedRow(rows)) {
        context.output(outputRow);
      }
    } catch (Exception e) {
      for (Row outputRow : rows) {
        context.output(
            failureTag,
            FailsafeElement.of(outputRow, outputRow)
                .setErrorMessage(e.getMessage())
                .setStacktrace(Throwables.getStackTraceAsString(e)));
      }

    }
}
```

with

```java
private void processBufferedRows(Iterable<Row> rows, WindowedContext context) {
  for (Row outputRow : rows) {
    context.output(row);
  }
    
}
```

Next step: metadata file.

<!-- ------------------------ -->
## Template metadata
Duration: 0:05:00

Dataflow templates can provide a JSON metadata file with the template description and supported parameters. 

#### Metadata parameters

| Parameter Key	 | Required | Description of the value |
| --- | ----------- | --- |
| name | Yes | The name of your template. |
| description | No | A short paragraph of text describing the templates. |
| parameters | No. Defaults to an empty array. | An array of additional parameters that will be used by the template. |

#### Template parameters description in metadata file

| Parameter Key	 | Required | Description of the value |
| --- | ----------- | --- |
| name | Yes | 	The name of the parameter used in your template. |
| label | Yes | A human readable label that will be used in the UI to label the parameter. |
| helpText | Yes | A short paragraph of text describing the parameter. |
| isOptional | No. Defaults to false. | *false* if the parameter is required and *true* if the parameter is optional. |
| regexes | No. Defaults to an empty array. | An array of POSIX-egrep regular expressions in string form that will be used to validate the value of the parameter. For example: `["^[a-zA-Z][a-zA-Z0-9]+"]` is a single regular expression that validates that the value starts with a letter and then has one or more characters. |

#### Example metadata file

```json
{
  "description": "An example pipeline that counts words in the input file.",
  "name": "Word Count",
  "parameters": [
    {
      "regexes": [
        "^gs:\\/\\/[^\\n\\r]+$"
      ],
      "name": "inputFile",
      "helpText": "Path of the file pattern glob to read from. ex: gs://dataflow-samples/shakespeare/kinglear.txt",
      "label": "Input Cloud Storage file(s)"
    },
    {
      "regexes": [
        "^gs:\\/\\/[^\\n\\r]+$"
      ],
      "name": "output",
      "helpText": "Path and filename prefix for writing output files. ex: gs://MyBucket/counts",
      "label": "Output Cloud Storage file(s)"
    }
  ]
}
```

#### Add PARQUET to supported formats in the metadata file

Navigate to and open in editor template metadata file: 

```
v2/protegrity-data-tokenization/src/main/resources/protegrity_data_tokenization_metadata.json
```

Find following parameters:
* inputGcsFileFormat
* outputGcsFileFormat

Add `PARQUET` to the regex section to validate `PARQUET` format correctly

```json
"regexes": [
        "^(JSON|CSV|AVRO|PARQUET)$"
]
```

Next step: build Dataflow template.

<!-- ------------------------ -->
## Build Dataflow Flex template
Duration: 0:05:00

Dataflow Flex templates package the pipeline as a Docker image and stages the docker image on your
project's [Container Registry](https://cloud.google.com/container-registry).

#### Assembling the Uber-JAR

The Dataflow Flex templates require your Java project to be built into an Uber JAR file.

Navigate to the v2 folder:

```
cd /path/to/DataflowTemplates/v2
```

Build the Uber JAR:

```
mvn package -am -pl protegrity-data-tokenization
```

ℹ️ An **Uber JAR** - also known as **fat JAR** - is a single JAR file that contains both target
package *and* all its dependencies.

The result of the `package` task execution is a `protegrity-data-tokenization-1.0-SNAPSHOT.jar`
file that is generated under the `target` folder protegrity-data-tokenization directory.

#### Creating the Dataflow Flex template

Navigate to the template folder:

```
cd /path/to/DataflowTemplates/v2/protegrity-data-tokenization
```

Build command to build the Dataflow Flex template:

```
gcloud dataflow flex-template build ${TEMPLATE_PATH} \
       --image-gcr-path "${TARGET_GCR_IMAGE}" \
       --sdk-language "JAVA" \
       --flex-template-base-image ${BASE_CONTAINER_IMAGE} \
       --metadata-file "src/main/resources/protegrity_data_tokenization_metadata.json" \
       --jar "target/protegrity-data-tokenization-1.0-SNAPSHOT.jar" \
       --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.teleport.v2.templates.ProtegrityDataTokenization"
```
* `TEMPLATE_PATH` path on GCS where manifest for template will be located.
* `image-gcr-path` path in GCR, should be in format  `gcr.io/${PROJECT}/${IMAGE_NAME}`
* `sdk-language` might be Java or Python
* `flex-template-base-image` You must use a Google-provided base image to package your containers using Docker. Choose the most recent version name from the [Flex Templates base images reference](https://cloud.google.com/dataflow/docs/reference/flex-templates-base-images). Or use shortcuts like `JAVA8`
* `metadata-file` path to metadata file in this case: `resources/protegrity_data_tokenization_metadata.json`
* `jar` path to **UBER JAR** 
* `FLEX_TEMPLATE_JAVA_MAIN_CLASS` path to the main class. In this case: `com.google.cloud.teleport.v2.templates.ProtegrityDataTokenization`

Next step: run Dataflow template on Google Cloud
<!-- ------------------------ -->
## Run Dataflow template on Google Cloud
Duration: 0:10:00

To deploy the pipeline, refer to the template metadata file and pass the [parameters](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options) required by the pipeline.

The template requires set of the parameters, and to test parquet format support as an input source
you need to specify few of them.

- **dataSchemaGcsPath**: Path to data schema file located on GCS. BigQuery compatible JSON format data schema required
  - [This sample schema](https://github.com/akvelon/codelabs/blob/main/beam-college-dataflow/resources/schema-example.json)
    can be used for testing - just copy it to your GCS bucket
- **inputGcsFilePattern**: GCS file pattern for files in the source bucket
  - [This data sample](https://github.com/akvelon/codelabs/blob/main/beam-college-dataflow/resources/data-example.json)
  can be used for testing - just copy it to your GCS bucket
- **inputGcsFileFormat**: File format of the input files. Supported formats: JSON, CSV, Avro and **PARQUET**
- **dsgUri**: URI for the DSG API calls, if you don't have this one. Just skip it.
- **outputGcsDirectory**: GCS bucket folder to write data to
- **outputGcsFileFormat**: File format of output files. Supported formats: JSON, CSV, Avro and **PARQUET**

A Dataflow job can be created and executed from this template in 3 ways:

1. Using [Dataflow Google Cloud Console](https://console.cloud.google.com/dataflow/jobs)
2. Using `gcloud` CLI tool

```bash
gcloud dataflow flex-template run "protegrity-data-tokenization-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "${TEMPLATE_PATH}" \
    --parameters <parameter>="<value>" \
    --parameters <parameter>="<value>" \
    ...
    --parameters <parameter>="<value>" \
    --region "${REGION}"
```

3. With a REST API request

```bash
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/locations/${REGION}/flexTemplates:launch"
JOB_NAME="protegrity-data-tokenization-`date +%Y%m%d-%H%M%S-%N`"

time curl -X POST -H "Content-Type: application/json" \
    -H "Authorization: Bearer $(gcloud auth print-access-token)" \
    -d '
     {
         "launch_parameter": {
             "jobName": "'$JOB_NAME'",
             "containerSpecGcsPath": "'$TEMPLATE_PATH'",
             "parameters": {
                 "<parameter>": "<value>",
                 "<parameter>": "<value>",
                 ...
                 "<parameter>": "<value>"
             }
         }
     }
    '
    "${TEMPLATES_LAUNCH_API}"
```

You can find more information in Google [documentation](https://cloud.google.com/dataflow/docs/guides/templates/running-templates) for Dataflow Template. 

## Congratulations!
Duration: 0:01:00

Now you can use this template to read parquet files, tokenize the information from them, and then write the tokenized
data into other parquet files. Moreover, you've learned how to easily combine any formats, input sources and output sinks
using Beam Row concept.

#### What you've learned during this codelab:

* Dataflow Flex template structure
* How to use Beam Schema API and Beam Row in particular
* What the steps are to support many sources and formats
* How to build templates and run them in Google Dataflow

#### Learn More
* Dataflow documentation: [https://cloud.google.com/dataflow/docs/](https://cloud.google.com/dataflow/docs/)