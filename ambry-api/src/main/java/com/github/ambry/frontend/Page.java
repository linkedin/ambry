/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.frontend;

import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Represents a page of a list that may be sent as an API response.
 * @param <T> the type of the elements in the page
 */
public class Page<T> {
  private static final String ENTRIES_KEY = "entries";
  private static final String NEXT_PAGE_TOKEN_KEY = "nextPageToken";

  private final List<T> entries;
  private final String nextPageToken;

  /**
   *
   * @param entries the entries in this response page.
   * @param nextPageToken {@code null} if there are no remaining pages to read, or a string that represents an offset to
   *                          resume reading additional pages from.
   */
  public Page(List<T> entries, String nextPageToken) {
    this.entries = Collections.unmodifiableList(entries);
    this.nextPageToken = nextPageToken;
  }

  /**
   * @return the entries in this response page.
   */
  public List<T> getEntries() {
    return entries;
  }

  /**
   * @return {@code null} if there are no remaining pages to read, or a string that represents an offset to resume
   *         reading additional pages from.
   */
  public String getNextPageToken() {
    return nextPageToken;
  }

  /**
   * @param entrySerializer a function that serializes an entry in the page into JSON.
   * @return the {@link JSONObject} representing the page.
   */
  public JSONObject toJson(Function<T, JSONObject> entrySerializer) {
    JSONArray entriesArray = new JSONArray();
    entries.stream().map(entrySerializer).forEach(entriesArray::put);
    return new JSONObject().put(ENTRIES_KEY, entriesArray).putOpt(NEXT_PAGE_TOKEN_KEY, nextPageToken);
  }

  /**
   * @param jsonObject the {@link JSONObject} representing the page.
   * @param entryDeserializer a function that deserializes JSON for entries in the page.
   * @return the {@link Page} that was deserialized.
   */
  public static <T> Page<T> fromJson(JSONObject jsonObject, Function<JSONObject, T> entryDeserializer) {
    JSONArray entriesArray = jsonObject.optJSONArray(ENTRIES_KEY);
    List<T> entries = entriesArray == null ? Collections.emptyList()
        : Utils.listView(entriesArray::length, i -> entryDeserializer.apply(entriesArray.getJSONObject(i)));
    String nextPageToken = jsonObject.optString(NEXT_PAGE_TOKEN_KEY, null);
    return new Page<>(entries, nextPageToken);
  }
}
