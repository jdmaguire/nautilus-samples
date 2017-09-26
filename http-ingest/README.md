# Sample Ingestion Webapp

This sample illustrates how to build an ingestion gateway as a webapp. The webapp serves a simple API to allow ingesting data into any Pravega stream.

# Ingestion API

By default the webapp listens on port 9099 but that is configurable. The app provides a single API method.

## POST /{scope}/{stream}?key={key}

Ingests the payload into Pravega. This will create the stream if it does not already exist and ingest the raw content of the http payload into Pravega.

Parameters:

* Path Parameters:
    * scope - The pravega scope to write to is encoded as part of the URL.
    * stream - The pravega stream to write to is encoded as part of the URL.
* Query Parameters:
    * key - Optional. The routing key is set via a query parameter.

# Running in Nautilus

You must first have the following pre-requisites installed.

* Nautilus CLI - Available from the Nautilus UI through the user dropdown menu.
* DC/OS CLI - Available through the DC/OS UI in the menu dropdown.

Both of these tools should be on the path.

First target the Nautilus cluster and login.

```
nautilus target <TARGET_IP>
nautilus login
# .. enter username/password
```

Ensure you add the Target cluster IP as a docker insecure registry (or 0.0.0.0). This is located in preferences/daemon for docker for mac.

Then you can run the deploy script to deploy the app to marathon. This script will compile the app, create the docker container and deploy it to Nautilus using marathon:

```
./deploy.sh <TARGET_IP>
```

## Testing the Webapp

You can now test out the app by running the following curl command from the Nautilus cluster.

```
curl -d "Hello World!" "http://httpingest.marathon.mesos:9099/example/ingest?key=hello"
```

Try this out using different messages, keys and streams.
