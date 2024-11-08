# EMR-engineering-ETL-pipeline
This repository contains an Apache Airflow-based workflow designed for processing large-scale data using key big data frameworks. The pipeline automates the provisioning of an EMR cluster, performs data extraction and transformation using Apache Spark, and terminates the cluster once all tasks are complete. It is built to handle both structured and unstructured data at scale, leveraging the power of Hadoop Distributed File System (HDFS) for storage and Spark for processing large datasets.

# Key Big Data Frameworks:
1. Apache Spark: A fast, in-memory data processing engine used for large-scale data transformations and analytics. This pipeline uses Spark to perform data extraction and complex transformations on large datasets.
2. Hadoop Distributed File System (HDFS): The underlying distributed storage system used by EMR clusters to store and manage massive volumes of data across multiple nodes, ensuring scalability and fault tolerance.

# Features:
1. Automated EMR cluster provisioning and termination
2. Data extraction and transformation using Apache Spark
3. Monitoring of EMR job flow and steps
4. Seamless integration with S3 and Snowflake for storage and processing
5. Robust error handling and retry mechanisms
   
# Technologies:
1. Apache Airflow for workflow orchestration
2. AWS EC2, S3, EMR, and Snowflake for cloud-based storage and processing
3. Apache Spark for big data processing and transformations
4. Python for scripting custom transformations and job execution

# Useful links
1. https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/emr/emr.html
2. https://registry.astronomer.io/providers/amazon/versions/latest/modules/emrjobflowsensor
3. https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html
4. https://registry.astronomer.io/providers/amazon/versions/latest/modules/emraddstepsoperator
