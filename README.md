# Nautilus Samples

This repository contains sample [Flink](https://flink.apache.org/) applications that integrate with [Pravega](http://pravega.io/) and are optimized for use in Nautilus.

## Getting Started

### Optional - Build Pravega & Flink Connectors

Building the samples will compile against the Pravega 0.1.0 and 0.1.0 Connectors releases. If you need to use a different version of these, you will need to compile them yourself.

Follow the steps below to build and publish artifacts from source to local Maven repository.

```
$ git clone https://github.com/pravega/pravega.git
$ cd pravega
$ ./gradlew clean install

$ git clone https://github.com/pravega/flink-connectors.git
$ cd flink-connectors
$ ./gradlew clean install
```

Alternatively, follow the instructions from [here](http://pravega.io/docs/getting-started/) to pull from release repository.

### Build the Sample Code

Follow the below steps to build the sample code:

```
$ git clone https://github.com/pravega/nautilus-samples.git
$ cd nautilus-samples
$ ./gradlew clean installDist
```

## Anomaly Detection

The anomaly detection sample is an application that simulates network anomaly intrusion and detection using Apache Flink and Apache Pravega. It contains details [instructions](anomaly-detection) on how to deploy, configure and run this sample on Nautilus.
