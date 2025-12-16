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
 */

package com.github.ambry.clustermap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test for {@link NimbusServiceMetadata}.
 */
public class NimbusServiceMetadataTest {

  private Path tempDir;

  @Before
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("nimbus-test");
  }

  @After
  public void tearDown() throws IOException {
    if (tempDir != null) {
      Files.walk(tempDir)
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  /**
   * Test successful reading of valid nimbus service metadata.
   */
  @Test
  public void testReadFromFileValid() throws IOException {
    String jsonContent = "{\n" +
        "  \"appInstanceID\": \"test-app-123\",\n" +
        "  \"nodeName\": \"test-node-01\",\n" +
        "  \"maintenanceZone\": \"zone-a\"\n" +
        "}";

    File metadataFile = createTempFile("nimbus-service.json", jsonContent);
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());

    assertNotNull("Metadata should not be null", metadata);
    assertEquals("App instance ID should match", "test-app-123", metadata.getAppInstanceID());
    assertEquals("Node name should match", "test-node-01", metadata.getNodeName());
    assertEquals("Maintenance zone should match", "zone-a", metadata.getMaintenanceZone());
  }

  /**
   * Test reading with missing optional fields.
   */
  @Test
  public void testReadFromFilePartialData() throws IOException {
    String jsonContent = "{\n" +
        "  \"appInstanceID\": \"test-app-456\"\n" +
        "}";

    File metadataFile = createTempFile("nimbus-service-partial.json", jsonContent);
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());

    assertNotNull("Metadata should not be null", metadata);
    assertEquals("App instance ID should match", "test-app-456", metadata.getAppInstanceID());
    assertNull("Node name should be null", metadata.getNodeName());
    assertNull("Maintenance zone should be null", metadata.getMaintenanceZone());
  }

  /**
   * Test reading with extra unknown fields (should be ignored).
   */
  @Test
  public void testReadFromFileWithUnknownFields() throws IOException {
    String jsonContent = "{\n" +
        "  \"appInstanceID\": \"test-app-789\",\n" +
        "  \"nodeName\": \"test-node-02\",\n" +
        "  \"maintenanceZone\": \"zone-b\",\n" +
        "  \"unknownField\": \"should-be-ignored\",\n" +
        "  \"anotherUnknownField\": 12345\n" +
        "}";

    File metadataFile = createTempFile("nimbus-service-extra.json", jsonContent);
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());

    assertNotNull("Metadata should not be null", metadata);
    assertEquals("App instance ID should match", "test-app-789", metadata.getAppInstanceID());
    assertEquals("Node name should match", "test-node-02", metadata.getNodeName());
    assertEquals("Maintenance zone should match", "zone-b", metadata.getMaintenanceZone());
  }

  /**
   * Test reading from null file path.
   */
  @Test
  public void testReadFromFileNullPath() {
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(null);
    assertNull("Metadata should be null for null path", metadata);
  }

  /**
   * Test reading from empty file path.
   */
  @Test
  public void testReadFromFileEmptyPath() {
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile("");
    assertNull("Metadata should be null for empty path", metadata);

    metadata = NimbusServiceMetadata.readFromFile("   ");
    assertNull("Metadata should be null for whitespace path", metadata);
  }

  /**
   * Test reading from non-existent file.
   */
  @Test
  public void testReadFromFileNonExistent() {
    String nonExistentPath = tempDir.resolve("non-existent-file.json").toString();
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(nonExistentPath);
    assertNull("Metadata should be null for non-existent file", metadata);
  }

  /**
   * Test reading from unreadable file.
   */
  @Test
  public void testReadFromFileUnreadable() throws IOException {
    File metadataFile = createTempFile("unreadable.json", "{}");
    metadataFile.setReadable(false);

    try {
      NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());
      // On some systems, setReadable(false) might not work, so we check if it actually became unreadable
      if (!metadataFile.canRead()) {
        assertNull("Metadata should be null for unreadable file", metadata);
      }
    } finally {
      metadataFile.setReadable(true); // Restore for cleanup
    }
  }

  /**
   * Test reading from file with invalid JSON.
   */
  @Test
  public void testReadFromFileInvalidJson() throws IOException {
    String invalidJsonContent = "{\n" +
        "  \"appInstanceID\": \"test-app\",\n" +
        "  \"nodeName\": \"test-node\"\n" +
        "  // missing closing brace";

    File metadataFile = createTempFile("invalid.json", invalidJsonContent);
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());
    assertNull("Metadata should be null for invalid JSON", metadata);
  }

  /**
   * Test reading from empty JSON file.
   */
  @Test
  public void testReadFromFileEmptyJson() throws IOException {
    File metadataFile = createTempFile("empty.json", "{}");
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());

    assertNotNull("Metadata should not be null for empty JSON", metadata);
    assertNull("App instance ID should be null", metadata.getAppInstanceID());
    assertNull("Node name should be null", metadata.getNodeName());
    assertNull("Maintenance zone should be null", metadata.getMaintenanceZone());
  }

  /**
   * Test getters with null values.
   */
  @Test
  public void testGettersWithNullValues() throws IOException {
    File metadataFile = createTempFile("null-values.json", "{}");
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());

    assertNotNull("Metadata should not be null", metadata);
    assertNull("App instance ID should be null", metadata.getAppInstanceID());
    assertNull("Node name should be null", metadata.getNodeName());
    assertNull("Maintenance zone should be null", metadata.getMaintenanceZone());
  }

  /**
   * Test toString method.
   */
  @Test
  public void testToString() throws IOException {
    String jsonContent = "{\n" +
        "  \"appInstanceID\": \"test-app-toString\",\n" +
        "  \"nodeName\": \"test-node-toString\",\n" +
        "  \"maintenanceZone\": \"zone-toString\"\n" +
        "}";

    File metadataFile = createTempFile("toString-test.json", jsonContent);
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());

    String result = metadata.toString();
    assertTrue("Should contain app instance ID", result.contains("test-app-toString"));
    assertTrue("Should contain node name", result.contains("test-node-toString"));
    assertTrue("Should contain maintenance zone", result.contains("zone-toString"));
    assertTrue("Should contain class name", result.contains("NimbusServiceMetadata"));
  }

  /**
   * Test toString method with null values.
   */
  @Test
  public void testToStringWithNullValues() throws IOException {
    File metadataFile = createTempFile("toString-null.json", "{}");
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());

    String result = metadata.toString();
    assertTrue("Should contain null values", result.contains("null"));
    assertTrue("Should contain class name", result.contains("NimbusServiceMetadata"));
  }

  /**
   * Test reading with different JSON formatting.
   */
  @Test
  public void testReadFromFileCompactJson() throws IOException {
    String compactJsonContent = "{\"appInstanceID\":\"compact-app\",\"nodeName\":\"compact-node\",\"maintenanceZone\":\"compact-zone\"}";

    File metadataFile = createTempFile("compact.json", compactJsonContent);
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());

    assertNotNull("Metadata should not be null", metadata);
    assertEquals("App instance ID should match", "compact-app", metadata.getAppInstanceID());
    assertEquals("Node name should match", "compact-node", metadata.getNodeName());
    assertEquals("Maintenance zone should match", "compact-zone", metadata.getMaintenanceZone());
  }

  /**
   * Test reading with special characters in values.
   */
  @Test
  public void testReadFromFileSpecialCharacters() throws IOException {
    String jsonContent = "{\n" +
        "  \"appInstanceID\": \"app-with-special-chars-@#$%\",\n" +
        "  \"nodeName\": \"node_with_underscores_123\",\n" +
        "  \"maintenanceZone\": \"zone.with.dots\"\n" +
        "}";

    File metadataFile = createTempFile("special-chars.json", jsonContent);
    NimbusServiceMetadata metadata = NimbusServiceMetadata.readFromFile(metadataFile.getAbsolutePath());

    assertNotNull("Metadata should not be null", metadata);
    assertEquals("App instance ID should handle special chars", "app-with-special-chars-@#$%", metadata.getAppInstanceID());
    assertEquals("Node name should handle underscores", "node_with_underscores_123", metadata.getNodeName());
    assertEquals("Maintenance zone should handle dots", "zone.with.dots", metadata.getMaintenanceZone());
  }

  /**
   * Helper method to create a temporary file with given content.
   */
  private File createTempFile(String fileName, String content) throws IOException {
    File file = tempDir.resolve(fileName).toFile();
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }
}
