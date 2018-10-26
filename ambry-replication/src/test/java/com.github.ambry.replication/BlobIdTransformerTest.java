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
package com.github.ambry.replication;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.DeleteMessageFormatInputStream;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.messageformat.MetadataContentSerDe;
import com.github.ambry.messageformat.PutMessageFormatBlobV1InputStream;
import com.github.ambry.messageformat.PutMessageFormatInputStream;
import com.github.ambry.store.Message;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MockStoreKeyConverterFactory;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import com.github.ambry.store.TransformationOutput;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests the BlobIdTransformer
 */
public class BlobIdTransformerTest {
  private final ClusterMap clusterMap = new MockReadingClusterMap();
  private final BlobIdFactory blobIdFactory = new BlobIdFactory(clusterMap);
  private final BlobIdTransformer transformer;
  private final List<Pair> pairList;
  private final MockStoreKeyConverterFactory factory;
  private static final int BLOB_STREAM_SIZE = 128;
  private static final int BLOB_ENCRYPTION_KEY_SIZE = 32;
  private static final int USER_META_DATA_SIZE = 64;
  public static final Pair<String, String> BLOB_ID_PAIR_VERSION_1_CONVERTED =
      new Pair<>("AAEAAQAAAAAAAAAhAAAAJDkwNTUwOTJhLTc3ZTAtNDI4NC1iY2IxLTc2MDZlYTAzNWM4OQ",
          "AAMB_wE5AAIAAQAAAAAAAAAhAAAAJDkwNTUwOTJhLTc3ZTAtNDI4NC1iY2IxLTc2MDZlYTAzNWM4OQ");
  public static final Pair<String, String> BLOB_ID_PAIR_VERSION_2_CONVERTED =
      new Pair<>("AAIAAQB8AAIAAQAAAAAAAAAbAAAAJDRiYTE0YzFkLTFjNmUtNDYyNC04ZDcyLTU3ZDQzZjgzOWM4OQ",
          "AAMBAQB8AAIAAQAAAAAAAAAbAAAAJDRiYTE0YzFkLTFjNmUtNDYyNC04ZDcyLTU3ZDQzZjgzOWM4OQ");
  public static final Pair<String, String> BLOB_ID_PAIR_VERSION_3_CONVERTED =
      new Pair<>("AAMAAgCgAAMAAQAAAAAAAACEAAAAJDYwMmQ0ZGQxLTQ5NDUtNDg0YS05MmQwLTI5YjVkM2ZlOWM4OQ",
          "AAMBAgCgAAIAAQAAAAAAAACEAAAAJDYwMmQ0ZGQxLTQ5NDUtNDg0YS05MmQwLTI5YjVkM2ZlOWM4OQ");
  public static final Pair<String, String> BLOB_ID_PAIR_VERSION_3_NULL =
      new Pair<>("AAMAAAAAAAAAAAAAAAAAAAAAAAAAJDNlM2U1YzY0LTgxMWItNDVlZi04N2QzLTgyZmZmOWRmNTIxOA", null);
  public static final String VERSION_1_UNCONVERTED =
      "AAEAAQAAAAAAAABZAAAAJGYwMjRiYzIyLTA4NDMtNGNjMC1iMzNiLTUyOGZmZTA4NWM4OQ";
  public static final String VERSION_3_UNCONVERTED =
      "AAMAAAAAAAAAAAAAAAAAAAAAAAAAJDUyYTk2OWIyLTA3YWMtNDBhMC05ZmY2LTUxY2ZkZjY4NWM4OQ";

  //All these Metadata related pairs have the same
  //account ID: 313 and the same container ID: 2
  public static final Pair<String, String> BLOB_ID_VERSION_1_METADATA_CONVERTED =
      new Pair<>("AAEAAQAAAAAAAABSAAAAJGQ2YjM0YzI2LWU0MjMtNGNkNC1iMGZhLTU5Yzc2YmVhZjk2ZA",
          "AAMB_wE5AAIAAQAAAAAAAABSAAAAJGQ2YjM0YzI2LWU0MjMtNGNkNC1iMGZhLTU5Yzc2YmVhZjk2ZA");
  public static final Pair<String, String> BLOB_ID_VERSION_1_METADATA_UNCONVERTED =
      new Pair<>("AAEAAQAAAAAAAABiAAAAJGVlM2YzYjFkLTA4NDEtNGZmMS04MGVmLTU4MWM4ZWIwNjkzOQ",
          "AAEAAQAAAAAAAABiAAAAJGVlM2YzYjFkLTA4NDEtNGZmMS04MGVmLTU4MWM4ZWIwNjkzOQ");
  public static final Pair<String, String> BLOB_ID_VERSION_1_DATACHUNK_0_CONVERTED =
      new Pair<>("AAEAAQAAAAAAAABlAAAAJDJjNzhmYTYxLTlhZDQtNDg2YS1iZTZkLWFlMGE0ODNjNTI2YQ",
          "AAMB_wE5AAIAAQAAAAAAAABlAAAAJDJjNzhmYTYxLTlhZDQtNDg2YS1iZTZkLWFlMGE0ODNjNTI2YQ");
  public static final Pair<String, String> BLOB_ID_VERSION_1_DATACHUNK_1_CONVERTED =
      new Pair<>("AAEAAQAAAAAAAAAkAAAAJGQyZmYxMDE5LTBmMDQtNDEwNi05NDBjLWY5ZTgwYTU2ZmY1YQ",
          "AAMB_wE5AAIAAQAAAAAAAAAkAAAAJGQyZmYxMDE5LTBmMDQtNDEwNi05NDBjLWY5ZTgwYTU2ZmY1YQ");
  public static final Pair<String, String> BLOB_ID_VERSION_1_DATACHUNK_1_UNCONVERTED =
      new Pair<>("AAEAAQAAAAAAAAAHAAAAJGIxZmYwYmE5LTMwYTAtNDY0OC05MzUyLWZjYWViY2M4YTgzMQ",
          "AAEAAQAAAAAAAAAHAAAAJGIxZmYwYmE5LTMwYTAtNDY0OC05MzUyLWZjYWViY2M4YTgzMQ");

  private static final Class[] VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS =
      new Class[]{PutMessageFormatInputStream.class, PutMessageFormatBlobV1InputStream.class};

  /**
   * Sets up common components
   * @throws IOException
   */
  public BlobIdTransformerTest() throws Exception {
    Pair<String, String>[] pairs =
        new Pair[]{BLOB_ID_PAIR_VERSION_1_CONVERTED, BLOB_ID_PAIR_VERSION_2_CONVERTED, BLOB_ID_PAIR_VERSION_3_CONVERTED, BLOB_ID_PAIR_VERSION_3_NULL, BLOB_ID_VERSION_1_METADATA_CONVERTED, BLOB_ID_VERSION_1_DATACHUNK_0_CONVERTED, BLOB_ID_VERSION_1_DATACHUNK_1_CONVERTED, BLOB_ID_VERSION_1_DATACHUNK_1_UNCONVERTED, BLOB_ID_VERSION_1_METADATA_UNCONVERTED};
    factory = new MockStoreKeyConverterFactory(null, null);
    factory.setReturnInputIfAbsent(true);
    StoreKeyConverter storeKeyConverter = createAndSetupMockStoreKeyConverter(factory, pairs);
    transformer = new BlobIdTransformer(blobIdFactory, storeKeyConverter);
    pairList = new ArrayList<>(Arrays.asList(pairs));
    pairList.add(new Pair<>(VERSION_3_UNCONVERTED, VERSION_3_UNCONVERTED));
    preConvertPairFirsts(pairList, storeKeyConverter);
  }

  /**
   * Tests basic use of transformer with blobs that can be converted and those that aren't
   * @throws Exception
   */
  @Test
  public void testBasicOperation() throws Exception {
    for (Pair pair : pairList) {
      for (Class clazz : VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS) {
        for (boolean divergeInfoFromData : new boolean[]{false, true}) {
          InputAndExpected inputAndExpected = new InputAndExpected(pair, clazz, divergeInfoFromData);
          TransformationOutput output = transformer.transform(inputAndExpected.getInput());
          assertNull("output exception should be null", output.getException());
          verifyOutput(output.getMsg(), inputAndExpected.getExpected());
        }
      }
    }
  }

  /**
   * Tests metadata blob transformation
   * @throws IOException
   * @throws MessageFormatException
   */
  @Test
  public void testMetaDataBlobOperation() throws IOException, MessageFormatException {
    InputAndExpected inputAndExpected =
        new InputAndExpected(BLOB_ID_VERSION_1_METADATA_CONVERTED, VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0], false,
            new String[]{BLOB_ID_VERSION_1_DATACHUNK_0_CONVERTED.getFirst(), BLOB_ID_VERSION_1_DATACHUNK_1_CONVERTED.getFirst()},
            new String[]{BLOB_ID_VERSION_1_DATACHUNK_0_CONVERTED.getSecond(), BLOB_ID_VERSION_1_DATACHUNK_1_CONVERTED.getSecond()});
    TransformationOutput output = transformer.transform(inputAndExpected.getInput());
    assertNull("output exception should be null", output.getException());
    verifyOutput(output.getMsg(), inputAndExpected.getExpected());
  }

  /**
   * Tests that correct exception is made when transformation is attempted
   * on a metadata chunk with a deprecated data chunk
   * @throws IOException
   * @throws MessageFormatException
   */
  @Test
  public void testBrokenDeprecatedMetaDataBlobOperation() throws IOException, MessageFormatException {
    InputAndExpected inputAndExpected =
        new InputAndExpected(pairList.get(0), VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0], false,
            new String[]{BLOB_ID_PAIR_VERSION_3_NULL.getFirst(), BLOB_ID_PAIR_VERSION_3_CONVERTED.getFirst()}, null);
    assertException(transformer.transform(inputAndExpected.getInput()), IllegalStateException.class);
  }

  /**
   * Tests that correct exception is made when transformation is attempted
   * on a changed metadata chunk with an unchanged data chunk
   * @throws IOException
   * @throws MessageFormatException
   */
  @Test
  public void testBrokenUnchangedMetaDataBlobOperation() throws IOException, MessageFormatException {
    InputAndExpected inputAndExpected =
        new InputAndExpected(BLOB_ID_VERSION_1_METADATA_CONVERTED, VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0], false,
            new String[]{BLOB_ID_VERSION_1_DATACHUNK_0_CONVERTED.getFirst(), BLOB_ID_VERSION_1_DATACHUNK_1_UNCONVERTED.getFirst()},
            null);
    assertException(transformer.transform(inputAndExpected.getInput()), IllegalStateException.class);
  }

  /**
   * Tests that correct exception is made when transformation is attempted
   * on a unchanged metadata chunk with an changed data chunk
   * @throws IOException
   * @throws MessageFormatException
   */
  @Test
  public void testBrokenChangedMetaDataBlobOperation() throws IOException, MessageFormatException {
    InputAndExpected inputAndExpected =
        new InputAndExpected(BLOB_ID_VERSION_1_METADATA_UNCONVERTED, VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0], false,
            new String[]{BLOB_ID_VERSION_1_DATACHUNK_0_CONVERTED.getFirst(), BLOB_ID_VERSION_1_DATACHUNK_1_UNCONVERTED.getFirst()},
            null);
    assertException(transformer.transform(inputAndExpected.getInput()), IllegalStateException.class);
  }

  /**
   * Tests that correct exception is made when transformation is attempted
   * on a changed metadata chunk with an datachunks with a different account ID / container ID
   * @throws IOException
   * @throws MessageFormatException
   */
  @Test
  public void testBrokenDifferentAccountIdContainerIdMetaDataBlobOperation()
      throws IOException, MessageFormatException {
    InputAndExpected inputAndExpected =
        new InputAndExpected(pairList.get(0), VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0], false,
            new String[]{BLOB_ID_PAIR_VERSION_2_CONVERTED.getFirst(), BLOB_ID_PAIR_VERSION_3_CONVERTED.getFirst()},
            null);
    assertException(transformer.transform(inputAndExpected.getInput()), IllegalStateException.class);
  }

  /**
   * Tests a non-put message input to the transformer
   * @throws Exception
   */
  @Test
  public void testNonPutTransform() throws Exception {
    InputAndExpected inputAndExpected =
        new InputAndExpected(pairList.get(0), DeleteMessageFormatInputStream.class, false);
    assertException(transformer.transform(inputAndExpected.getInput()), IllegalArgumentException.class);
  }

  /**
   * Tests putting in garbage input in the message inputStream into the transformer
   * @throws Exception
   */
  @Test
  public void testGarbageInputStream() throws Exception {
    InputAndExpected inputAndExpected = new InputAndExpected(pairList.get(0), null, false);
    assertException(transformer.transform(inputAndExpected.getInput()), MessageFormatException.class);
  }

  /**
   * Tests transformer when the underlying StoreKeyConverter isn't working
   * @throws Exception
   */
  @Test
  public void testBrokenStoreKeyConverter() throws Exception {
    InputAndExpected inputAndExpected =
        new InputAndExpected(pairList.get(0), VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0], false);
    TransformationOutput output = transformer.transform(inputAndExpected.getInput());
    verifyOutput(output.getMsg(), inputAndExpected.getExpected());

    factory.setException(new BlobIdTransformerTestException());
    inputAndExpected = new InputAndExpected(pairList.get(1), VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0], false);
    output = transformer.transform(inputAndExpected.getInput());
    Assert.assertTrue("Should lead to IllegalStateException", output.getException() instanceof IllegalStateException);
    factory.setException(null);
    inputAndExpected = new InputAndExpected(pairList.get(2), VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0], false);
    output = transformer.transform(inputAndExpected.getInput());
    verifyOutput(output.getMsg(), inputAndExpected.getExpected());
  }

  /**
   * Tests creating the transformer with a null StoreKeyConverter
   */
  @Test
  public void testNullStoreKeyConverter() throws IOException {
    try {
      new BlobIdTransformer(blobIdFactory, null);
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
      new BlobIdTransformer(null, factory.getStoreKeyConverter());
      fail("Did not throw NullPointerException");
    } catch (NullPointerException e) {
      //expected
    }
  }

  /**
   * Tests BlobIdTransformer's warmup() method
   * @throws Exception
   */
  @Test
  public void testWarmup() throws Exception {
    InputAndExpected inputAndExpected =
        new InputAndExpected(pairList.get(0), VALID_MESSAGE_FORMAT_INPUT_STREAM_IMPLS[0], true);
    BlobIdTransformer transformer = new BlobIdTransformer(blobIdFactory, factory.getStoreKeyConverter());
    TransformationOutput output = transformer.transform(inputAndExpected.getInput());
    Assert.assertTrue("Should lead to IllegalStateException", output.getException() instanceof IllegalStateException);
    transformer.warmup(Collections.singletonList(inputAndExpected.getInput().getMessageInfo()));
    output = transformer.transform(inputAndExpected.getInput());
    assertNull(output.getException());
    verifyOutput(output.getMsg(), inputAndExpected.getExpected());
  }

  /**
   * Tests using the transformer with null input to the transform method
   * @throws Exception
   */
  @Test
  public void testNullTransformInput() throws Exception {
    assertException(transformer.transform(null), NullPointerException.class);
  }

  /**
   * Tests using the transformer with Message inputs that have null components
   * @throws Exception
   */
  @Test
  public void testNullComponentsTransformInput() throws Exception {
    MessageInfo messageInfo = new MessageInfo(createBlobId(VERSION_1_UNCONVERTED), 123, (short) 123, (short) 123, 0L);
    //null msgBytes
    Message message = new Message(messageInfo, null);
    assertException(transformer.transform(message), NullPointerException.class);
    //null messageInfo
    message = new Message(null, new ByteArrayInputStream(new byte[30]));
    assertException(transformer.transform(message), NullPointerException.class);
  }

  private BlobId createBlobId(String hexBlobId) throws IOException {
    if (hexBlobId == null) {
      return null;
    }
    return new BlobId(hexBlobId, clusterMap);
  }

  private StoreKeyConverter createAndSetupMockStoreKeyConverter(MockStoreKeyConverterFactory factory,
      Pair<String, String>[] pairs) throws Exception {
    Map<StoreKey, StoreKey> map = new HashMap<>();
    for (Pair<String, String> pair : pairs) {
      map.put(createBlobId(pair.getFirst()), createBlobId(pair.getSecond()));
    }
    factory.setConversionMap(map);
    return factory.getStoreKeyConverter();
  }

  /**
   * Runs all the {@link Pair#getFirst()} outputs from the {@link Pair} list through
   * the {@link StoreKeyConverter} storeKeyConverter.
   * Intended to be run so that the StoreKeyConverter's
   * {@link StoreKeyConverter#getConverted(StoreKey)} method can
   * work on any of the pairs' getFirst() outputs.
   * @param pairs
   * @param storeKeyConverter
   * @throws Exception {@link StoreKeyConverter#convert(Collection)} may throw an Exception
   */
  private void preConvertPairFirsts(List<Pair> pairs, StoreKeyConverter storeKeyConverter) throws Exception {
    List<StoreKey> pairFirsts = new ArrayList<>();
    for (Pair<String, String> pair : pairs) {
      pairFirsts.add(createBlobId(pair.getFirst()));
    }
    storeKeyConverter.convert(pairFirsts);
  }

  private void verifyOutput(Message output, Message expected) throws IOException {
    if (expected == null) {
      assertNull("output should be null", output);
    } else {
      assertEquals("MessageInfos not equal", expected.getMessageInfo(), output.getMessageInfo());
      TestUtils.assertInputStreamEqual(expected.getStream(), output.getStream(),
          (int) expected.getMessageInfo().getSize(), true);
    }
  }

  private void assertException(TransformationOutput transformationOutput, Class exceptionClass) {
    assertNull("Message in output is not null", transformationOutput.getMsg());
    assertTrue("Exception from output is not " + exceptionClass.getName(),
        exceptionClass.isInstance(transformationOutput.getException()));
  }

  /**
   * Creates a random Message input and a related expected Message output
   */
  private class InputAndExpected {

    private final Message input;
    private final Message expected;

    private final long randomStaticSeed = new Random().nextLong();
    private Random buildRandom = new Random(randomStaticSeed);

    /**
     * Constructs the input and expected
     * @param pair the pair of blob ids (old, new)
     * @param clazz the put message input stream class to use
     * @param divergeInfoFromData if {@code true}, changes some fields in the info to be different from what is in the
     *                            data
     * @throws IOException
     * @throws MessageFormatException
     */
    InputAndExpected(Pair<String, String> pair, Class clazz, boolean divergeInfoFromData)
        throws IOException, MessageFormatException {
      this(pair, clazz, divergeInfoFromData, null, null);
    }

    InputAndExpected(Pair<String, String> pair, Class clazz, boolean divergeInfoFromData, String[] dataChunkIdsInput,
        String[] dataChunkIdsExpected) throws IOException, MessageFormatException {
      boolean hasEncryption = clazz == PutMessageFormatInputStream.class;
      Long crcInput = buildRandom.nextLong();
      input = buildMessage(pair.getFirst(), clazz, hasEncryption, crcInput, divergeInfoFromData, dataChunkIdsInput);
      if (pair.getSecond() == null) {
        //can't just assign 'input' since Message has an
        //InputStream that is modified when read
        expected = null;//buildMessage(pair.getFirst(), PutMessageFormatInputStream.class, hasEncryption);
      } else {
        Long crcExpected = pair.getSecond().equals(pair.getFirst()) ? crcInput : null;
        expected = buildMessage(pair.getSecond(), PutMessageFormatInputStream.class, hasEncryption, crcExpected,
            divergeInfoFromData, dataChunkIdsExpected);
      }
    }

    public Message getInput() {
      return input;
    }

    public Message getExpected() {
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

    private Message buildMessage(String blobIdString, Class clazz, boolean hasEncryption, Long crcInMsgInfo,
        boolean divergeInfoFromData, String... dataChunkIds) throws IOException, MessageFormatException {
      buildRandom = new Random(randomStaticSeed);

      //If there are datachunks, it's a metadata blob.
      //If not, its a data blob
      InputStream blobStream;
      long blobStreamSize;
      ByteBuffer byteBuffer;
      BlobType blobType;
      if (dataChunkIds == null) {
        blobStreamSize = BLOB_STREAM_SIZE;
        blobStream = createBlobStream();
        blobType = BlobType.DataBlob;
      } else {
        byteBuffer = createMetadataByteBuffer(dataChunkIds);
        blobStreamSize = byteBuffer.remaining();
        blobStream = new ByteBufferInputStream(byteBuffer);
        blobType = BlobType.MetadataBlob;
      }

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
          new BlobProperties(blobStreamSize, "serviceId", "ownerId", "contentType", false, 0, 0, blobId.getAccountId(),
              blobId.getContainerId(), hasEncryption);
      if (clazz != null) {
        MessageFormatInputStream messageFormatInputStream;
        if (clazz == PutMessageFormatInputStream.class) {
          messageFormatInputStream =
              new PutMessageFormatInputStream(blobId, blobEncryptionKey, blobProperties, userMetaData, blobStream,
                  blobStreamSize, blobType);
        } else if (clazz == DeleteMessageFormatInputStream.class) {
          messageFormatInputStream =
              new DeleteMessageFormatInputStream(blobId, blobId.getAccountId(), blobId.getContainerId(), 0);
        } else {//if (clazz == PutMessageFormatBlobV1InputStream.class) {
          messageFormatInputStream =
              new PutMessageFormatBlobV1InputStream(blobId, blobProperties, userMetaData, blobStream, blobStreamSize,
                  blobType);
        }
        inputStreamSize = (int) messageFormatInputStream.getSize();
        inputStream = messageFormatInputStream;
      } else {
        inputStream = new ByteArrayInputStream(randomByteArray(100));
        inputStreamSize = 100;
      }

      boolean ttlUpdated = false;
      long expiryTimeMs =
          Utils.addSecondsToEpochTime(blobProperties.getCreationTimeInMs(), blobProperties.getTimeToLiveInSeconds());
      if (divergeInfoFromData) {
        ttlUpdated = true;
        expiryTimeMs = Utils.Infinite_Time;
      }
      messageInfo =
          new MessageInfo(blobId, inputStreamSize, false, ttlUpdated, expiryTimeMs, crcInMsgInfo, blobId.getAccountId(),
              blobId.getContainerId(), blobProperties.getCreationTimeInMs());
      return new Message(messageInfo, inputStream);
    }

    private InputStream createBlobStream() {
      return new ByteArrayInputStream(randomByteArray(BLOB_STREAM_SIZE));
    }

    /**
     * Creates metadata blob data buffer from supplied datachunkIds
     * @param datachunkIds
     * @return
     * @throws IOException
     */
    private ByteBuffer createMetadataByteBuffer(String... datachunkIds) throws IOException {
      List<StoreKey> storeKeys = new ArrayList<>();
      for (String datachunkId : datachunkIds) {
        storeKeys.add(blobIdFactory.getStoreKey(datachunkId));
      }
      ByteBuffer output = MetadataContentSerDe.serializeMetadataContent(4000000, 8000000, storeKeys);
      output.flip();
      return output;
    }
  }

  private class BlobIdTransformerTestException extends Exception {
  }
}
