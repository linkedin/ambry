/*
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend.s3;

public class S3Constants {
  public static final int MIN_PART_NUM = 1;
  public static final int MAX_PART_NUM = 10000;
  public static final int MAX_LIST_SIZE = 10000;

  // Error Messages
  public static final String ERR_INVALID_PART_NUMBER =
      "Invalid part number: %d. " + String.format("Part number must be an integer between %d and %d.", MIN_PART_NUM, MAX_PART_NUM);
  public static final String ERR_DUPLICATE_PART_NUMBER = "Duplicate part number found: %d.";
  public static final String ERR_DUPLICATE_ETAG = "Duplicate eTag found: %s.";
  public static final String ERR_EMPTY_REQUEST_BODY = "Xml request body cannot be empty.";
  public static final String ERR_PART_LIST_TOO_LONG = String.format("Parts list size cannot exceed %d.", MAX_LIST_SIZE);
  public static final String ERR_INVALID_PART_LIST = "Failed to parse xml request body due error %s";
}
