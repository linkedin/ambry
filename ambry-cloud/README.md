# ambry-cloud

This package contains classes that enable a Virtual Cloud Replica (VCR)
to reflect partition-level replication changes to a remote cloud backup.
Currently Azure is supported; other vendors will be added later.

Components

* CloudBlobStore: a Store implementation that works with a CloudDestination
 to reflect blob operations to a cloud store.
* CloudDestination: interface for transmitting blob data and metadata between the
local cluster and a remote cloud storage engine.
* AzureCloudDestination: a CloudDestination implementation that stores blob data
in Azure Blob Storage, and metadata in Azure Cosmos DB.
