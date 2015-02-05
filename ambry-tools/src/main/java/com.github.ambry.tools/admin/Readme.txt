This document describes about various tools in this package and their usages

DumpData
This tool is used to dump the log or index replicaToken. This tool is also used to compare index entries to log dump

To excute this tool, copy all the required jar files to some temp directory. Once you have all the required jar files in
your temp directory, you can execute the following commands.


Dumping Index:

Dump Index:
java -Xms4g -Xmx4g -XX:NewSize=500m -XX:MaxNewSize=500m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:SurvivorRatio=128
-verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:InitialTenuringThreshold=15 -XX:MaxTenuringThreshold=15
-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -Xloggc:gc.log
 -cp "*" com.github.ambry.tools.admin.DumpData --hardwareLayout [HardwareLayoutFile]
 --partitionLayout [PartitionLayoutFile] --typeOfFile index --fileToRead [indexFile]

Dump index filtering for a list of blobs:
java -Xms4g -Xmx4g -XX:NewSize=500m -XX:MaxNewSize=500m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:SurvivorRatio=128
-verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:InitialTenuringThreshold=15 -XX:MaxTenuringThreshold=15
-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -Xloggc:gc.log -cp "*"
com.github.ambry.tools.admin.DumpData --hardwareLayout [HardwareLayoutFile] --partitionLayout [PartitionLayoutFile]
--typeOfFile index --fileToRead [indexFile] --listOfBlobs [blobid1,blobid2,blobid3]

Dumping Log:

Dump log file:
java -Xms4g -Xmx4g -XX:NewSize=500m -XX:MaxNewSize=500m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
-XX:SurvivorRatio=128 -verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:InitialTenuringThreshold=15
-XX:MaxTenuringThreshold=15 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
-Xloggc:gc.log -cp "*" com.github.ambry.tools.admin.DumpData --hardwareLayout [HardwareLayoutFile]
--partitionLayout [PartitionLayoutFile] --typeOfFile index --fileToRead [logFile]

Dump log ending at offset x:
java -Xms4g -Xmx4g -XX:NewSize=500m -XX:MaxNewSize=500m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
-XX:SurvivorRatio=128 -verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:InitialTenuringThreshold=15
-XX:MaxTenuringThreshold=15 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
-Xloggc:gc.log -cp "*" com.github.ambry.tools.admin.DumpData
--hardwareLayout [HardwareLayoutFile] --partitionLayout [PartitionLayoutFile] --typeOfFile index
--fileToRead [logFile] --endOffset x

dump log starting at x offset and ending at y filtered with a set of blobs:
java -Xms4g -Xmx4g -XX:NewSize=500m -XX:MaxNewSize=500m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
-XX:SurvivorRatio=128 -verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:InitialTenuringThreshold=15
-XX:MaxTenuringThreshold=15 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
-Xloggc:gc.log -cp "*" com.github.ambry.tools.admin.DumpData
--hardwareLayout [HardwareLayoutFile] --partitionLayout [PartitionLayoutFile] --typeOfFile index
--fileToRead [logFile] --startOffset x --endOffset y --listOfBlobs [blobid1,blobid2,blobid3]

Comparing index entries to log entries(or in other words, trying to find index entry in the log with index info):

java -Xms4g -Xmx4g -XX:NewSize=500m -XX:MaxNewSize=500m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
-XX:SurvivorRatio=128 -verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:InitialTenuringThreshold=15
-XX:MaxTenuringThreshold=15 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
-Xloggc:gc.log -cp "*" com.github.ambry.tools.admin.DumpData
--hardwareLayout [HardwareLayoutFile] --partitionLayout [PartitionLayoutFile] --fileToRead [indexFile]
--logFileToDump [logFile]



BlobInfoTool:

This tool is used to perform read operations for a blob. Features supported so far are:
1. List partitions for a blobid
2. Get blob(deserialize blob) for a given blob id for all its replicas
3. Get blob(deserialize blob) for a given blob id for local replicas
4. Get blob(deserialize blob) for a given blob id for the given replica


List Replica:
java -Xms4g -Xmx4g -XX:NewSize=500m -XX:MaxNewSize=500m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
-XX:SurvivorRatio=128 -verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:InitialTenuringThreshold=15
-XX:MaxTenuringThreshold=15 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
-Xloggc:gc.log -cp "*" com.github.ambry.tools.admin.BlobInfoTool
--hardwareLayout [HardwareLayoutFile] --partitionLayout [PartitionLayoutFile] --typeOfOperation LIST_REPLICAS
--ambryBlobId [blobid]

Get blob from all replicas:
java -Xms4g -Xmx4g -XX:NewSize=500m -XX:MaxNewSize=500m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
-XX:SurvivorRatio=128 -verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:InitialTenuringThreshold=15
-XX:MaxTenuringThreshold=15 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
-Xloggc:gc.log -cp "*" com.github.ambry.tools.admin.BlobInfoTool --hardwareLayout [HardwareLayoutFile]
--partitionLayout [PartitionLayoutFile] --typeOfOperation GET_BLOB_FROM_ALL_REPLICAS --ambryBlobId [blobid]

Get blob from a local replica:
java -Xms4g -Xmx4g -XX:NewSize=500m -XX:MaxNewSize=500m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
-XX:SurvivorRatio=128 -verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:InitialTenuringThreshold=15
-XX:MaxTenuringThreshold=15 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
-Xloggc:gc.log -cp "*" com.github.ambry.tools.admin.BlobInfoTool --hardwareLayout [HardwareLayoutFile]
--partitionLayout [PartitionLayoutFile] --typeOfOperation GET_BLOB_FROM_LOCAL_REPLICA --fabric [fabric]
--ambryBlobId [blobid]

Get blob from a replica:
java -Xms4g -Xmx4g -XX:NewSize=500m -XX:MaxNewSize=500m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC
-XX:SurvivorRatio=128 -verbose:gc -XX:+PrintGCApplicationStoppedTime -XX:InitialTenuringThreshold=15
-XX:MaxTenuringThreshold=15 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
-Xloggc:gc.log -cp "*" com.github.ambry.tools.admin.BlobInfoTool --hardwareLayout [HardwareLayoutFile]
--partitionLayout [PartitionLayoutFile] --typeOfOperation GET_BLOB_FROM_REPLICA --ambryBlobId [blobid]
--replicaHost [replicaHost] --replicaPort [replicaPort]





