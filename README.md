# Apache Beam Example Pipelines

## Description
This project contains three example pipelines that demonstrate 
some of the capabilities of Apache Beam.

## Running the example

### Project setup
Please follow the steps below to run the example:
1. Configure `gcloud` with your credentials 
2. Enable Cloud Dataflow API in your 
Google Cloud Platform project 

#### Batch pipeline:
Run the following command to execute the batch pipeline:
```
python -m complete.batch_pipeline.batch_pipeline \
--input gs://[DATA FILE BUCKET]/users.csv \
--output [PROJECT ID]:beam.users \
--temp_location gs://[DATAFLOW STAGING BUCKET]/temp/ \
--staging_location gs://[DATAFLOW STAGING BUCKET]/stage/ \
--project [PROJECT ID] \
--runner DataflowRunner
```

### Sum pipeline:
Run the following command to execute the sum pipeline:
```
python -m template.sum_pipeline.sum_pipeline \
--input gs://[DATA FILE BUCKET]/retail.csv \
--output [PROJECT ID]:beam.retail \
--temp_location gs://[DATAFLOW STAGING BUCKET]/temp/ \
--staging_location gs://[DATAFLOW STAGING BUCKET]/stage/ \
--project [PROJECT ID] \
--runner DataflowRunner
```

### Streaming pipeline:
Run the following command to execute the streaming pipeline:
```
python -m template.streaming_pipeline.streaming_pipeline \
--input projects/[PROJECT ID]/topics/[TOPIC NAME] \
--output [PROJECT ID]:beam.streaming_sum \
--temp_location gs://[DATAFLOW STAGING BUCKET]/temp/ \
--staging_location gs://[DATAFLOW STAGING BUCKET]/stage/ \
--project [PROJECT ID] \
--runner DataflowRunner \
--streaming
```
