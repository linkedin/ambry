/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.store;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.PutMessageFormatBlobV1InputStream;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.utils.Utils;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests the BlobIdTransformer
 */
public class BlobIdTransformerTest {

  private final ClusterMap clusterMap = new MockReadingClusterMap();

  private final BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);

  private final BlobIdTransformer transformer;

  private final List<BlobIdStringPair> blobIdStringPairList;

  private final MockStoreKeyConverter mockStoreKeyConverter;

  private static final int BLOB_STREAM_SIZE = 128;
  private static final int BLOB_ENCRYPTION_KEY_SIZE = 32;
  private static final int USER_META_DATA_SIZE = 64;
  public static final BlobIdStringPair BLOB_ID_PAIR_VERSION_1_CONVERTED = new BlobIdStringPair("AAEAAQAAAAAAAAAhAAAAJDkwNTUwOTJhLTc3ZTAtNDI4NC1iY2IxLTc2MDZlYTAzNWM4OQ", "AAMB_wE5AAIAAQAAAAAAAAAhAAAAJDkwNTUwOTJhLTc3ZTAtNDI4NC1iY2IxLTc2MDZlYTAzNWM4OQ");
  public static final BlobIdStringPair BLOB_ID_PAIR_VERSION_2_CONVERTED = new BlobIdStringPair("AAIAAQB8AAIAAQAAAAAAAAAbAAAAJDRiYTE0YzFkLTFjNmUtNDYyNC04ZDcyLTU3ZDQzZjgzOWM4OQ", "AAMBAQB8AAIAAQAAAAAAAAAbAAAAJDRiYTE0YzFkLTFjNmUtNDYyNC04ZDcyLTU3ZDQzZjgzOWM4OQ");
  public static final BlobIdStringPair BLOB_ID_PAIR_VERSION_3_CONVERTED = new BlobIdStringPair("AAMAAgCgAAMAAQAAAAAAAACEAAAAJDYwMmQ0ZGQxLTQ5NDUtNDg0YS05MmQwLTI5YjVkM2ZlOWM4OQ", "AAMBAgCgAAIAAQAAAAAAAACEAAAAJDYwMmQ0ZGQxLTQ5NDUtNDg0YS05MmQwLTI5YjVkM2ZlOWM4OQ");
  public static final String VERSION_1_UNCONVERTED = "AAEAAQAAAAAAAABZAAAAJGYwMjRiYzIyLTA4NDMtNGNjMC1iMzNiLTUyOGZmZTA4NWM4OQ";
  public static final String VERSION_3_UNCONVERTED = "AAMAAAAAAAAAAAAAAAAAAAAAAAAAJDUyYTk2OWIyLTA3YWMtNDBhMC05ZmY2LTUxY2ZkZjY4NWM4OQ";

  private static final Class[] VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS =
      new Class[]{PutMessageFormatInputStream.class, PutMessageFormatBlobV1InputStream.class};

  /**
   * Sets up common components
   * @throws IOException
   */
  public BlobIdTransformerTest() throws IOException {
    BlobIdStringPair[] blobIdStringPairs = new BlobIdStringPair[]{BLOB_ID_PAIR_VERSION_1_CONVERTED, BLOB_ID_PAIR_VERSION_2_CONVERTED, BLOB_ID_PAIR_VERSION_3_CONVERTED};
    mockStoreKeyConverter = createAndSetupMockStoreKeyConverter(blobIdStringPairs);
    transformer = new BlobIdTransformer(mockStoreKeyConverter, blobIdFactory, null);
    blobIdStringPairList = new ArrayList<>(Arrays.asList(blobIdStringPairs));
    blobIdStringPairList.add(new BlobIdStringPair(VERSION_3_UNCONVERTED, null));
  }

  /**
   * Tests basic use of transformer with blobs that can be converted and those that aren't
   * @throws Exception
   */
  @Test
  public void testBasicOperation() throws Exception {
    for (BlobIdStringPair blobIdStringPair : blobIdStringPairList) {
      for (Class clazz : VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS) {
        InputAndExpected inputAndExpected = new InputAndExpected(blobIdStringPair, clazz);
        StoreCopier.Message output = transformer.transform(inputAndExpected.getInput());
        verifyOutput(output, inputAndExpected.getExpected());
      }
    }
  }

  /**
   * Tests a non-put message input to the transformer
   * @throws Exception
   */
  @Test
  public void testNonPutTransform() throws Exception {
    InputAndExpected inputAndExpected = new InputAndExpected(blobIdStringPairList.get(0), DeleteMessageFormatInputStream.class);
    try {
      transformer.transform(inputAndExpected.getInput());
      fail("Did not throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  /**
   * Tests putting in garbage input in the message inputStream into the transformer
   * @throws Exception
   */
  @Test
  public void testGarbageInputStream() throws Exception {
    InputAndExpected inputAndExpected = new InputAndExpected(blobIdStringPairList.get(0), null);
    try {
      transformer.transform(inputAndExpected.getInput());
      fail("Did not throw MessageFormatException");
    } catch (MessageFormatException e) {
      //expected
    }
  }

  /**
   * Tests transformer when the underlying StoreKeyConverter isn't working
   * @throws Exception
   */
  @Test
  public void testBrokenStoreKeyConverter() throws Exception {
    InputAndExpected inputAndExpected =
        new InputAndExpected(blobIdStringPairList.get(0), VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0]);
    StoreCopier.Message output = transformer.transform(inputAndExpected.getInput());
    verifyOutput(output, inputAndExpected.getExpected());

    mockStoreKeyConverter.setThrowException(true);
    inputAndExpected = new InputAndExpected(blobIdStringPairList.get(1), VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0]);
    try {
      transformer.transform(inputAndExpected.getInput());
      fail("Did not throw MockStoreKeyConverterException");
    } catch (MockStoreKeyConverter.MockStoreKeyConverterException e) {
      //exception expected
    }

    mockStoreKeyConverter.setThrowException(false);
    inputAndExpected = new InputAndExpected(blobIdStringPairList.get(2), VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0]);
    output = transformer.transform(inputAndExpected.getInput());
    verifyOutput(output, inputAndExpected.getExpected());
  }

  /**
   * Tests creating the transformer with a null StoreKeyConverter
   */
  @Test
  public void testNullStoreKeyConverter() throws IOException {
    try {
      new BlobIdTransformer(null, blobIdFactory, null);
      fail("Did not throw NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  /**
   * Tests creating the transformer with a null StoreKeyFactory
   */
  @Test
  public void testNullStoreKeyFactory() throws IOException {
    try {
      new BlobIdTransformer(mockStoreKeyConverter, null, null);
      fail("Did not throw NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  /**
   * Tests using the transformer with null input to the transform method
   * @throws Exception
   */
  @Test
  public void testNullTransformInput() throws Exception {
    try {
      transformer.transform(null);
      fail("Did not throw NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  /**
   * Tests using the transformer with StoreCopier.Message inputs that have null components
   * @throws Exception
   */
  @Test
  public void testNullComponentsTransformInput() throws Exception {
    MessageInfo messageInfo =
        new MessageInfo(createBlobId(VERSION_1_UNCONVERTED), 123, (short) 123, (short) 123, 0L);
    InputStream inputStream = null;
    //null inputStream
    StoreCopier.Message message = new StoreCopier.Message(messageInfo, inputStream);
    try {
      transformer.transform(message);
      fail("Did not throw NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
    //null messageInfo
    message = new StoreCopier.Message(null, new ByteArrayInputStream(new byte[30]));
    try {
      transformer.transform(message);
      fail("Did not throw NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  private BlobId createBlobId(String hexBlobId) throws IOException {
    return new BlobId(hexBlobId, clusterMap);
  }

  private void putRowInStoreKeyConverter(MockStoreKeyConverter storeKeyConverter, BlobIdStringPair blobIdStringPair) throws IOException {
    storeKeyConverter.put(createBlobId(blobIdStringPair.getUnconverted()), createBlobId(blobIdStringPair.getConverted()));
  }

  private MockStoreKeyConverter createAndSetupMockStoreKeyConverter(BlobIdStringPair[] blobIdStringPairs) throws IOException {
    MockStoreKeyConverter storeKeyConverter = new MockStoreKeyConverter();
    for (BlobIdStringPair blobIdStringPair : blobIdStringPairs) {
      putRowInStoreKeyConverter(storeKeyConverter, blobIdStringPair);
    }
    return storeKeyConverter;
  }

  private String notEqualPrint(int loc, byte[] actual, byte[] expect) {
    StringBuilder sb = new StringBuilder();
    sb.append("Not equal at byte ")
        .append(loc)
        .append("\n")
        .append("Actual: ")
        .append(Hex.encodeHexString(actual))
        .append("\n")
        .append("Expect: ")
        .append(Hex.encodeHexString(expect))
        .append("\n");
    return sb.toString();
  }

  private void assertInputStreamEqual(InputStream actual, InputStream expect) throws IOException {
    byte[] actualBuf = inputStreamToByteArray(actual);
    byte[] expectBuf = inputStreamToByteArray(expect);
    assertTrue("Lengths are not equal, actual: " + actualBuf.length + ", expect: " + expectBuf.length,
        actualBuf.length == expectBuf.length);
    for (int i = 0; i < actualBuf.length; i++) {
      assertEquals(notEqualPrint(i, actualBuf, expectBuf), expectBuf[i], actualBuf[i]);
    }
  }

  private byte[] inputStreamToByteArray(InputStream is) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    int nRead;
    byte[] data = new byte[16384];

    //should be '!= -1' instead of '> 0', but MessageFormatInputStream
    //currently doesn't return -1 with the read(byte[], int, int) method
    while ((nRead = is.read(data, 0, data.length)) > 0) {
      buffer.write(data, 0, nRead);
    }
    buffer.flush();
    return buffer.toByteArray();
  }

  private void verifyOutput(StoreCopier.Message output, StoreCopier.Message expected) throws IOException {
    assertTrue("MessageInfos not equal, Output: " + output.getMessageInfo() + " Expected: " + expected.getMessageInfo(),
        output.getMessageInfo().equals(expected.getMessageInfo()));
    assertInputStreamEqual(output.getStream(), expected.getStream());
  }

  /**
   * Creates a random StoreCopier.Message input and a related expected StoreCopier.Message output
   */
  private class InputAndExpected {

    private final StoreCopier.Message input;
    private final StoreCopier.Message expected;

    private final long randomStaticSeed = new Random().nextLong();
    private Random buildRandom = new Random(randomStaticSeed);

    public InputAndExpected(BlobIdStringPair blobIdStringPair, Class clazz) throws IOException, MessageFormatException {
      boolean hasEncryption = clazz == PutMessageFormatInputStream.class ? true : false;
      input = buildMessage(blobIdStringPair.getUnconverted(), clazz, hasEncryption);
      if (blobIdStringPair.getConverted() == null) {
        //can't just assign 'input' since StoreCopier.Message has an
        //InputStream that is modified when read
        expected = buildMessage(blobIdStringPair.getUnconverted(), PutMessageFormatInputStream.class, hasEncryption);
      } else {
        expected = buildMessage(blobIdStringPair.getConverted(), PutMessageFormatInputStream.class, hasEncryption);
      }
    }

    public StoreCopier.Message getInput() {
      return input;
    }

    public StoreCopier.Message getExpected() {
      return expected;
    }

    private byte[] randomByteArray(int size) {
      byte[] bytes = new byte[size];
      buildRandom.nextBytes(bytes);
      return bytes;
    }

    private ByteBuffer randomByteBuffer(int size) {
      return ByteBuffer.wrap(randomByteArray(size));
    }

    private StoreCopier.Message buildMessage(String blobIdString, Class clazz, boolean hasEncryption)
        throws IOException, MessageFormatException {
      buildRandom = new Random(randomStaticSeed);
      BlobId blobId = createBlobId(blobIdString);
      ByteBuffer blobEncryptionKey = randomByteBuffer(BLOB_ENCRYPTION_KEY_SIZE);
      if (!hasEncryption) {
        blobEncryptionKey = null;
      }
      ByteBuffer userMetaData = randomByteBuffer(USER_META_DATA_SIZE);
      InputStream inputStream;
      int inputStreamSize;
      MessageInfo messageInfo;
      BlobProperties blobProperties =
          new BlobProperties(BLOB_STREAM_SIZE, "serviceId", "ownerId", "contentType", false, 0, 0,
              blobId.getAccountId(), blobId.getContainerId(), hasEncryption);
      if (clazz != null) {
        MessageFormatInputStream messageFormatInputStream;
        if (clazz == PutMessageFormatInputStream.class) {
          messageFormatInputStream =
              new PutMessageFormatInputStream(blobId, blobEncryptionKey, blobProperties, userMetaData,
                  createBlobStream(), (long) BLOB_STREAM_SIZE);
        } else if (clazz == DeleteMessageFormatInputStream.class) {
          messageFormatInputStream =
              new DeleteMessageFormatInputStream(blobId, blobId.getAccountId(), blobId.getContainerId(), 0);
        } else {//if (clazz == PutMessageFormatBlobV1InputStream.class) {
          messageFormatInputStream =
              new PutMessageFormatBlobV1InputStream(blobId, blobProperties, userMetaData, createBlobStream(),
                  (long) BLOB_STREAM_SIZE, BlobType.DataBlob);
        }
        inputStreamSize = (int) messageFormatInputStream.getSize();
        inputStream = messageFormatInputStream;
      } else {
        inputStream = new ByteArrayInputStream(randomByteArray(100));
        inputStreamSize = 100;
      }

      messageInfo = new MessageInfo(blobId, inputStreamSize, false, false,
          Utils.addSecondsToEpochTime(blobProperties.getCreationTimeInMs(), blobProperties.getTimeToLiveInSeconds()),
          null, blobId.getAccountId(), blobId.getContainerId(), blobProperties.getCreationTimeInMs());
      StoreCopier.Message message = new StoreCopier.Message(messageInfo, inputStream);
      return message;
    }

    private InputStream createBlobStream() {
      return new ByteArrayInputStream(randomByteArray(BLOB_STREAM_SIZE));
    }
  }

  public static class BlobIdStringPair {
    private final String unconverted;
    private final String converted;

    public BlobIdStringPair(String unconverted, String converted) {
      this.unconverted = unconverted;
      this.converted = converted;
    }

    public String getUnconverted() {
      return this.unconverted;
    }

    public String getConverted() {
      return this.converted;
    }

  }
}
