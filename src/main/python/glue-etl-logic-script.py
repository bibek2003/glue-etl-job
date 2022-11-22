#####Default Confuguration Block for Glue ETL: START : --> Do not Delete#####
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#####Default Confuguration Block for Glue ETL: END : --> Do not Delete#####

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATE1', 'DATE2'])  #==>Modify for optional arguments to be passed 

#####Default Confuguration Block for Glue ETL: START : --> Do not Delete#####
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
#####Default Confuguration Block for Glue ETL: END : --> Do not Delete#####

#####Custom python codes for log ingestion from S3 to Elasticsearch/Opensearch through Glue ETL jobs#######
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, helpers
import boto3
import json
import os
import datetime
import pandas as pd

hello_world = "This is a Test Hello World Program"
some_binary_data = b'Here we have some data'
more_binary_data = b'Here we have some more data'
bucket_name = "gs-glue-test-bucket"

# Method 1: Object.put()
s3 = boto3.resource('s3')
object = s3.Object(bucket_name, 'output/test-output-file1.txt')
object.put(Body=hello_world)
object.put(Body=some_binary_data)

# Method 2: Client.put_object()
client = boto3.client('s3')
client.put_object(Body=more_binary_data, Bucket=gs-glue-test-bucket, Key='output/test-output-file1.txt')   