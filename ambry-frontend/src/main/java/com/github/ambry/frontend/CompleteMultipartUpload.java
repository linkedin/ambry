/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;


public class CompleteMultipartUpload {

  @JacksonXmlProperty(localName = "Part")
  @JacksonXmlElementWrapper(useWrapping = false)
  private Part[] part;

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Part part : part) {
      sb.append(part).append(",");
    }
    return sb.toString();
  }

  public Part[] getPart() {
    return part;
  }

  public void setPart(Part[] part) {
    this.part = part;
  }
}
