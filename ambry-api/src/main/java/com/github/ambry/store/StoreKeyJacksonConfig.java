/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;


/**
 * A helper class to set up Jackson library for serialization and deserialization of store key.
 */
public class StoreKeyJacksonConfig {

  /**
   * Custom serializer for {@link StoreKey}.
   */
  public static class StoreKeySerializer extends StdSerializer<StoreKey> {
    protected StoreKeySerializer() {
      this(null);
    }

    protected StoreKeySerializer(Class<StoreKey> t) {
      super(t);
    }

    @Override
    public void serialize(StoreKey value, JsonGenerator gen, SerializerProvider provider) throws IOException {
      // Use the string format to serialize the store key.
      gen.writeString(value.getID());
    }
  }

  /**
   * Custom deserializer for {@link StoreKey}.
   */
  public static class StoreKeyDeserializer extends StdDeserializer<StoreKey> {
    private final StoreKeyFactory storeKeyFactory;

    protected StoreKeyDeserializer(StoreKeyFactory storeKeyFactory) {
      this(storeKeyFactory, null);
    }

    protected StoreKeyDeserializer(StoreKeyFactory storeKeyFactory, Class<?> vc) {
      super(vc);
      this.storeKeyFactory = storeKeyFactory;
    }

    @Override
    public StoreKey deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      JsonNode node = p.getCodec().readTree(p);
      // Node should be a string node
      String id = node.textValue();
      return storeKeyFactory.getStoreKey(id);
    }
  }

  /**
   * Set up {@link ObjectMapper} to use custom serializer and deserializer.
   * The reason to use this method instead of adding annotation directly to class, is because we need a {@link StoreKeyFactory}
   * to deserialize a string back to {@link StoreKey}.
   * @param objectMapper The {@link ObjectMapper}.
   * @param storeKeyFactory The {@link StoreKeyFactory}.
   */
  public static void setupObjectMapper(ObjectMapper objectMapper, StoreKeyFactory storeKeyFactory) {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(StoreKey.class, new StoreKeyDeserializer(storeKeyFactory));
    module.addSerializer(StoreKey.class, new StoreKeySerializer());
    objectMapper.registerModule(module);
  }
}
