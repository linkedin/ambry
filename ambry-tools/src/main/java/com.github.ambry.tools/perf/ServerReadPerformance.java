package com.github.ambry.tools.perf;

import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.messageformat.BlobOutput;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
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
              parser.accepts("logToRead", "The log that needs to be replayed for traffic")
                      .withRequiredArg()
                      .describedAs("log_to_read")
                      .ofType(String.class);

      ArgumentAcceptingOptionSpec<Integer> readsPerSecondOpt =
              parser.accepts("readsPerSecond", "The rate at which reads need to be performed")
                      .withRequiredArg()
                      .describedAs("The number of reads per second")
                      .ofType(Integer.class)
                      .defaultsTo(1000);

      ArgumentAcceptingOptionSpec<Boolean> verboseLoggingOpt =
              parser.accepts("enableVerboseLogging", "Enables verbose logging")
                      .withOptionalArg()
                      .describedAs("Enable verbose logging")
                      .ofType(Boolean.class)
                      .defaultsTo(false);

      ArgumentAcceptingOptionSpec<String> ambryHostOpt = parser.accepts("ambryHost", "The host to communicate")
              .withRequiredArg()
              .describedAs("ambry_host")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<Integer> ambryPortOpt = parser.accepts("ambryPort", "The port to communicate")
              .withRequiredArg()
              .describedAs("ambry_port")
              .ofType(Integer.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(logToReadOpt);
      listOpt.add(ambryHostOpt);
      listOpt.add(ambryPortOpt);

      for(OptionSpec opt : listOpt) {
        if(!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      String logToRead = options.valueOf(logToReadOpt);
      String host = options.valueOf(ambryHostOpt);
      int port = options.valueOf(ambryPortOpt);

      int readsPerSecond = options.valueOf(readsPerSecondOpt);
      boolean enableVerboseLogging = options.has(verboseLoggingOpt) ? true : false;
      if (enableVerboseLogging)
        System.out.println("Enabled verbose logging");

      final AtomicLong totalTimeTaken = new AtomicLong(0);
      final AtomicLong totalReads = new AtomicLong(0);
      final CountDownLatch latch = new CountDownLatch(4);
      final AtomicBoolean shutdown = new AtomicBoolean(false);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            System.out.println("Shutdown invoked");
            shutdown.set(true);
            System.out.println("Total reads : " + totalReads.get() + "  Total time taken : " + totalTimeTaken.get() +
                    " Nano Seconds  Average time taken per read " +
                    ((double)totalReads.get() / totalTimeTaken.get()) / SystemTime.NsPerSec + " Seconds");
          }
          catch (Exception e) {
            System.out.println("Error while shutting down " + e);
          }
        }
      });
      final BufferedReader br = new BufferedReader(new FileReader(logToRead));
      Throttler throttler = new Throttler(readsPerSecond, 100, true, SystemTime.getInstance());
      String line;
      Coordinator coordinator = new AmbryCoordinator(host, port);
      while ((line = br.readLine()) != null) {
        String[] id = line.split("\\|");
        System.out.println("calling get on " + id[1]);
        long startTime = System.currentTimeMillis();
        BlobOutput output = coordinator.getBlob(id[1]);
        System.out.println("Time taken to get " + (System.currentTimeMillis() - startTime));
        if (output != null) {
          long sizeRead = 0;
          byte[] outputBuffer = new byte[(int)output.getSize()];
          ByteBufferOutputStream streamOut = new ByteBufferOutputStream(ByteBuffer.wrap(outputBuffer));
          while (sizeRead < output.getSize()) {
            streamOut.write(output.getStream().read());
            sizeRead++;
          }
          // compare from source if present
          if (id.length == 4) {
            System.out.println("Comparing with source " + id[3]);
            File fileSource = new File(id[3]);
            FileInputStream fileInputStream = null;
            try {
              fileInputStream = new FileInputStream(fileSource);
              int sourceSize = (int)output.getSize();
              byte [] sourceBuffer = new byte[sourceSize];
              fileInputStream.read(sourceBuffer);
              if (Arrays.equals(sourceBuffer, outputBuffer)) {
                System.out.println("Equals");
              }
              else {
                System.out.println("Not equals");
              }

            }
            catch (Exception e) {
              System.out.println("Error while reading from source file " + e);
            }
            finally {
              if (fileInputStream != null) {
                fileInputStream.close();
              }
            }

          }
          throttler.maybeThrottle(1);
        }
      }
    }
    catch (Exception e) {
      System.out.println("Error in server read performance " + e);
    }
  }
}
