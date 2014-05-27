package com.github.ambry.tools.perf;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.ConnectionPoolConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.shared.BlobId;
import com.github.ambry.shared.BlockingChannelConnectionPool;
import com.github.ambry.shared.ConnectedChannel;
import com.github.ambry.shared.ConnectionPool;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Utils;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 *
 */
public class ServerReadPerformance {
  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> logToReadOpt =
          parser.accepts("logToRead", "The log that needs to be replayed for traffic").withRequiredArg()
              .describedAs("log_to_read").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<Integer> readsPerSecondOpt =
          parser.accepts("readsPerSecond", "The rate at which reads need to be performed").withRequiredArg()
              .describedAs("The number of reads per second").ofType(Integer.class).defaultsTo(1000);

      ArgumentAcceptingOptionSpec<Boolean> verboseLoggingOpt =
          parser.accepts("enableVerboseLogging", "Enables verbose logging").withOptionalArg()
              .describedAs("Enable verbose logging").ofType(Boolean.class).defaultsTo(false);

      ArgumentAcceptingOptionSpec<String> coordinatorConfigPathOpt =
          parser.accepts("coordinatorConfigPath", "The config for the coordinator").withRequiredArg()
              .describedAs("coordinator_config_path").ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(logToReadOpt);
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      listOpt.add(coordinatorConfigPathOpt);

      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      String logToRead = options.valueOf(logToReadOpt);
      String coordinatorConfigPath = options.valueOf(coordinatorConfigPathOpt);

      int readsPerSecond = options.valueOf(readsPerSecondOpt);
      boolean enableVerboseLogging = options.has(verboseLoggingOpt) ? true : false;
      if (enableVerboseLogging) {
        System.out.println("Enabled verbose logging");
      }
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath);

      final AtomicLong totalTimeTaken = new AtomicLong(0);
      final AtomicLong totalReads = new AtomicLong(0);
      final AtomicBoolean shutdown = new AtomicBoolean(false);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            System.out.println("Shutdown invoked");
            shutdown.set(true);
            System.out.println("Total reads : " + totalReads.get() + "  Total time taken : " + totalTimeTaken.get() +
                " Nano Seconds  Average time taken per read " +
                ((double) totalReads.get() / totalTimeTaken.get()) / SystemTime.NsPerSec + " Seconds");
          } catch (Exception e) {
            System.out.println("Error while shutting down " + e);
          }
        }
      });
      final BufferedReader br = new BufferedReader(new FileReader(logToRead));
      Throttler throttler = new Throttler(readsPerSecond, 100, true, SystemTime.getInstance());
      String line;
      ConnectedChannel channel = null;
      ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(new VerifiableProperties(new Properties()));
      ConnectionPool connectionPool = new BlockingChannelConnectionPool(connectionPoolConfig);
      long totalNumberOfGetBlobs = 0;
      long totalLatencyForGetBlobs = 0;
      long maxLatencyForGetBlobs = 0;
      long minLatencyForGetBlobs = Long.MAX_VALUE;

      while ((line = br.readLine()) != null) {
        String[] id = line.split("-");
        BlobOutput output = null;
        BlobId blobId = new BlobId(id[1], map);
        ArrayList<BlobId> blobIds = new ArrayList<BlobId>();
        blobIds.add(blobId);
        for (ReplicaId replicaId : blobId.getPartition().getReplicaIds()) {
          long startTimeGetBlob = 0;
          try {
            GetRequest getRequest =
                new GetRequest(1, "getperf", MessageFormatFlags.Blob, blobId.getPartition(), blobIds);
            channel = connectionPool
                .checkOutConnection(replicaId.getDataNodeId().getHostname(), replicaId.getDataNodeId().getPort(),
                    10000);
            startTimeGetBlob = SystemTime.getInstance().nanoseconds();
            channel.send(getRequest);
            InputStream receiveStream = channel.receive();
            GetResponse getResponse = GetResponse.readFrom(new DataInputStream(receiveStream), map);
            output = MessageFormatRecord.deserializeBlob(getResponse.getInputStream());
            long sizeRead = 0;
            byte[] outputBuffer = new byte[(int) output.getSize()];
            ByteBufferOutputStream streamOut = new ByteBufferOutputStream(ByteBuffer.wrap(outputBuffer));
            while (sizeRead < output.getSize()) {
              streamOut.write(output.getStream().read());
              sizeRead++;
            }
            long endTimeGetBlob = SystemTime.getInstance().nanoseconds() - startTimeGetBlob;
            totalNumberOfGetBlobs++;
            totalLatencyForGetBlobs += endTimeGetBlob;
            if (endTimeGetBlob > maxLatencyForGetBlobs) {
              maxLatencyForGetBlobs = endTimeGetBlob;
            }
            if (endTimeGetBlob < minLatencyForGetBlobs) {
              minLatencyForGetBlobs = endTimeGetBlob;
            }
            if (totalLatencyForGetBlobs >= 1000000000) {
              System.out.println(totalNumberOfGetBlobs + "    " + totalLatencyForGetBlobs * .001 + "    " +
                  maxLatencyForGetBlobs * .001 + "    " + minLatencyForGetBlobs * .001 + "    " +
                  ((double) totalLatencyForGetBlobs / totalNumberOfGetBlobs) * .001);
              totalLatencyForGetBlobs = 0;
              totalNumberOfGetBlobs = 0;
              maxLatencyForGetBlobs = 0;
              minLatencyForGetBlobs = Long.MAX_VALUE;
            }

            GetRequest getRequestProperties =
                new GetRequest(1, "getperf", MessageFormatFlags.BlobProperties, blobId.getPartition(), blobIds);
            long startTimeGetBlobProperties = SystemTime.getInstance().nanoseconds();
            channel.send(getRequestProperties);
            InputStream receivePropertyStream = channel.receive();
            GetResponse getResponseProperty = GetResponse.readFrom(new DataInputStream(receivePropertyStream), map);
            BlobProperties blobProperties =
                MessageFormatRecord.deserializeBlobProperties(getResponseProperty.getInputStream());
            long endTimeGetBlobProperties = SystemTime.getInstance().nanoseconds() - startTimeGetBlobProperties;

            GetRequest getRequestUserMetadata =
                new GetRequest(1, "getperf", MessageFormatFlags.BlobUserMetadata, blobId.getPartition(), blobIds);

            long startTimeGetBlobUserMetadata = SystemTime.getInstance().nanoseconds();
            channel.send(getRequestUserMetadata);
            InputStream receiveUserMetadataStream = channel.receive();
            GetResponse getResponseUserMetadata =
                GetResponse.readFrom(new DataInputStream(receiveUserMetadataStream), map);
            ByteBuffer userMetadata =
                MessageFormatRecord.deserializeUserMetadata(getResponseUserMetadata.getInputStream());
            long endTimeGetBlobUserMetadata = SystemTime.getInstance().nanoseconds() - startTimeGetBlobUserMetadata;
            throttler.maybeThrottle(1);
          } finally {
            if (channel != null) {
              connectionPool.checkInConnection(channel);
              channel = null;
            }
          }
        }
      }
    } catch (Exception e) {
      System.out.println("Error in server read performance " + e);
    }
  }
}
