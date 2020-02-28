# Ambry

[![Build Status](https://travis-ci.org/linkedin/ambry.svg?branch=master)](https://travis-ci.org/linkedin/ambry) 
[![codecov.io](https://codecov.io/github/linkedin/ambry/branch/master/graph/badge.svg)](https://codecov.io/github/linkedin/ambry)
[ ![Download](https://api.bintray.com/packages/linkedin/maven/ambry/images/download.svg) ](https://bintray.com/linkedin/maven/ambry/_latestVersion)
[![license](https://img.shields.io/github/license/linkedin/ambry.svg)](LICENSE)

Ambry is a distributed object store that supports storage of trillion of small immutable objects (50K -100K) as well as billions of large objects. It was specifically designed to store and serve media objects in web companies. However, it can be used as a general purpose storage system to store DB backups, search indexes or business reports. The system has the following characterisitics: 

1. Highly available and horizontally scalable
2. Low latency and high throughput
3. Optimized for both small and large objects
4. Cost effective
5. Easy to use

Requires at least JDK 1.8.
## Documentation
Detailed documentation is available at https://github.com/linkedin/ambry/wiki

## Research
Paper introducing Ambry at [SIGMOD 2016](http://sigmod2016.org/) -> http://dprg.cs.uiuc.edu/docs/SIGMOD2016-a/ambry.pdf

Reach out to us at ambrydev@googlegroups.com if you would like us to list a paper that is based off of research on Ambry.

## Getting Started
##### Step 1: Download the code, build it and prepare for deployment.
To get the latest code and build it, do

    $ git clone https://github.com/linkedin/ambry.git 
    $ cd ambry
    $ ./gradlew allJar
    $ cd target
    $ mkdir logs
Ambry uses files that provide information about the cluster to route requests from the frontend to servers and for replication between servers. We will use a simple clustermap that contains a single server with one partition. The partition will use `/tmp` as the mount point.
##### Step 2: Deploy a server.
    $ nohup java -Dlog4j.configuration=file:../config/log4j.properties -jar ambry.jar --serverPropsFilePath ../config/server.properties --hardwareLayoutFilePath ../config/HardwareLayout.json --partitionLayoutFilePath ../config/PartitionLayout.json > logs/server.log &

Through this command, we configure the log4j properties, provide the server with configuration options and cluster definitions and redirect output to a log. Note down the process ID returned (`serverProcessID`) because it will be needed for shutdown.  
The log will be available at `logs/server.log`. Alternately, you can change the log4j properties to write the log messages to a file instead of standard output.
##### Step 3: Deploy a frontend.
    $ nohup java -Dlog4j.configuration=file:../config/log4j.properties -cp "*" com.github.ambry.frontend.AmbryFrontendMain --serverPropsFilePath ../config/frontend.properties --hardwareLayoutFilePath ../config/HardwareLayout.json --partitionLayoutFilePath ../config/PartitionLayout.json > logs/frontend.log &

Note down the process ID returned (`frontendProcessID`) because it will be needed for shutdown. Make sure that the frontend is ready to receive requests.

    $ curl http://localhost:1174/healthCheck
    GOOD
The log will be available at `logs/frontend.log`. Alternately, you can change the log4j properties to write the log messages to a file instead of standard output.
##### Step 4: Interact with Ambry !
We are now ready to store and retrieve data from Ambry. Let us start by storing a simple image. For demonstration purposes, we will use an image `demo.gif` that has been copied into the `target` folder.
###### POST
    $ curl -i -H "x-ambry-service-id:CUrlUpload"  -H "x-ambry-owner-id:`whoami`" -H "x-ambry-content-type:image/gif" -H "x-ambry-um-description:Demonstration Image" http://localhost:1174/ --data-binary @demo.gif
    HTTP/1.1 201 Created
    Location: AmbryID
    Content-Length: 0
The CUrl command creates a `POST` request that contains the binary data in demo.gif. Along with the file data, we provide headers that act as blob properties. These include the size of the blob, the service ID, the owner ID and the content type.  
In addition to these properties, Ambry also has a provision for arbitrary user defined metadata. We provide `x-ambry-um-description` as user metadata. Ambry does not interpret this data and it is purely for user annotation.
The `Location` header in the response is the blob ID of the blob we just uploaded.
###### GET - Blob Info
Now that we stored a blob, let us verify some properties of the blob we uploaded.

    $ curl -i http://localhost:1174/AmbryID/BlobInfo
    HTTP/1.1 200 OK
    x-ambry-blob-size: {Blob size}
    x-ambry-service-id: CUrlUpload
    x-ambry-creation-time: {Creation time}
    x-ambry-private: false
    x-ambry-content-type: image/gif
    x-ambry-owner-id: {username}
    x-ambry-um-desc: Demonstration Image
    Content-Length: 0
###### GET - Blob
Now that we have verified that Ambry returns properties correctly, let us obtain the actual blob.

    $ curl http://localhost:1174/AmbryID > demo-downloaded.gif
    $ diff demo.gif demo-downloaded.gif 
    $
This confirms that the data that was sent in the `POST` request matches what we received in the `GET`. If you would like to see the image, simply point your browser to `http://localhost:1174/AmbryID` and you should see the image that was uploaded !
###### DELETE
Ambry is an immutable store and blobs cannot be updated but they can be deleted in order to make them irretrievable. Let us go ahead and delete the blob we just created.

    $ curl -i -X DELETE http://localhost:1174/AmbryID
    HTTP/1.1 202 Accepted
    Content-Length: 0
You will no longer be able to retrieve the blob properties or data.

    $ curl -i http://localhost:1174/AmbryID/BlobInfo
    HTTP/1.1 410 Gone
    Content-Type: text/plain; charset=UTF-8
    Content-Length: 17
    Connection: close

    Failure: 410 Gone
##### Step 5: Stop the frontend and server.
    $ kill -15 frontendProcessID
    $ kill -15 serverProcessID
You can confirm that the services have been shut down by looking at the logs.
##### Additional information:
In addition to the simple APIs demonstrated above, Ambry provides support for `GET` of only user metadata and `HEAD`. In addition to the `POST` of binary data that was demonstrated, Ambry also supports `POST` of `multipart/form-data` via CUrl or web forms.
Other features of interest include:
* **Time To Live (TTL)**: During `POST`, a TTL in seconds can be provided through the addition of a header named `x-ambry-ttl`. This means that Ambry will stop serving the blob after the TTL has expired. On `GET`, expired blobs behave the same way as deleted blobs.
* **Private**: During `POST`, providing a header named `x-ambry-private` with the value `true` will mark the blob as private. API behavior can be configured based on whether a blob is public or private.

