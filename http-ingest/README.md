# Sample Ingestion Webapp

This sample illustrates how to build an ingestion gateway as a webapp. The webapp serves a simple API to allow ingesting data into any Pravega stream.

## Running in Nautilus

TODO

# Ingestion API

By default the webapp listens on port 8080 but that is configurable. The app provides a single API method.

## POST /{scope}/{stream}?key={key}

Ingests the payload into Pravega. This will create the stream if it does not already exist and ingest the raw content of the http payload into Pravega.

Parameters:

* Path Parameters:
    * scope - The pravega scope to write to is encoded as part of the URL.
    * stream - The pravega stream to write to is encoded as part of the URL.
* Query Parameters:
    * key - Optional. The routing key is set via a query parameter.
