# Purpose

This repository serves as the baseline for our Supervised Machine Learning Model for Emotion Classifications

There are 2 main functions
1) `emoji.analysis`   
Streams data from Twitter and assigns and emotion to a Sentence based on Emojis. 
This is to provide additional training Data which is required for an accurate Supervised ML Model

2) `mlmodel`  
Actual Machine Learning Models

# Pre-requisites

```
Java 8
Hadoop 2.7
Spark 2.4.7
Scala 2.11.12
Zookeeper
Kafka
```

# Dev Setup

IDE : Intellij

```.env
File | Open from main Menu
```

```.env
Select build.sbt

Select as Project
``` 

# Run Emoji Analysis module

```
cd <path_to_cloned_repo_root_folder>
```
Build the jar file
```
sbt assembly
```

Navigate to the folder which contain the built jar file
```
cd target/scala-2.11
```
Run the jar file for the emoji.analysis module
```
java -cp machine-learning-assembly-0.2.jar emoji.analysis.EmojiCategory
```

Sync kafka topic data with S3 bucket run the following in the terminal
```
curl -X POST \
   localhost:8084/connectors \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d '{
  "name": "s3Connector",
  "config": {
   "connector.class": "io.confluent.connect.s3.S3SinkConnector",
  "s3.region": "us-east-2",
  "flush.size": "1000",
  "topics": "twitter.emojiAnalysis",
  "tasks.max": "1",
  "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "storage.class": "io.confluent.connect.s3.storage.S3Storage",
  "s3.bucket.name": "mtech-big-data",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter.schemas.enable": "false",
  "key.converter.schemas.enable": "false"
  }
}'
```