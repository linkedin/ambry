/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests the helper functions of {@link LogSegmentName}
 */
public class LogSegmentNameTest {

  /**
   * Tests the comparator in {@link LogSegmentName} for correctness.
   */
  @Test
  public void comparatorTest() {
    // compare empty name with empty name
    assertEquals("Empty names should be equal", 0,
        LogSegmentName.fromString("").compareTo(LogSegmentName.fromString("")));
    // create sample names
    LogSegmentName[] names =
        {LogSegmentName.fromPositionAndGeneration(0, 0), LogSegmentName.fromPositionAndGeneration(0, 1),
            LogSegmentName.fromPositionAndGeneration(1, 0), LogSegmentName.fromPositionAndGeneration(1, 1)};
    for (int i = 0; i < names.length; i++) {
      for (int j = 0; j < names.length; j++) {
        int expectCompare = Integer.compare(i, j);
        assertEquals("Unexpected value on compare", expectCompare, names[i].compareTo(names[j]));
        assertEquals("Unexpected value on compare", -1 * expectCompare, names[j].compareTo(names[i]));
      }
    }
    // empty name cannot be compared with anything else
    LogSegmentName validName = LogSegmentName.fromPositionAndGeneration(0, 0);
    try {
      validName.compareTo(LogSegmentName.fromString(""));
      fail("Should not have been able to compare empty name with anything else");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
    try {
      LogSegmentName.fromString("").compareTo(validName);
      fail("Should not have been able to compare empty name with anything else");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Checks the file name filter in {@link LogSegmentName} for correctness by creating valid and invalid files
   * and checking that the invalid ones are filtered out and the valid ones correctly picked up.
   * @throws IOException
   */
  @Test
  public void filenameFilterTest() throws IOException {
    int validFileCount = 10;
    int invalidFileCount = 5;
    Set<File> validFiles = new HashSet<>(validFileCount);
    File tempDir = Files.createTempDirectory("nameHelper-" + TestUtils.getRandomString(10)).toFile();
    tempDir.deleteOnExit();
    try {
      String filename = LogSegmentName.fromString("").toFilename();
      File file = createFile(tempDir, filename);
      validFiles.add(file);
      for (int i = 1; i < validFileCount; i++) {
        filename = StoreTestUtils.getRandomLogSegmentName(null).toFilename();
        file = createFile(tempDir, filename);
        validFiles.add(file);
      }
      for (int i = 0; i < invalidFileCount; i++) {
        filename = TestUtils.getRandomString(10);
        switch (i) {
          case 0:
            filename = filename + "_index";
            break;
          case 1:
            filename = filename + LogSegmentName.SUFFIX + "_temp";
            break;
          default:
            break;
        }
        createFile(tempDir, filename);
      }
      Set<File> filteredFiles = new HashSet<>(Arrays.asList(tempDir.listFiles(LogSegmentName.LOG_FILE_FILTER)));
      assertEquals("Filtered files do not have the valid files", validFiles, filteredFiles);
    } finally {
      File[] files = tempDir.listFiles();
      if (files != null) {
        for (File file : files) {
          assertTrue("The file [" + file.getAbsolutePath() + "] could not be deleted", file.delete());
        }
      }
      assertTrue("The directory [" + tempDir.getAbsolutePath() + "] could not be deleted", tempDir.delete());
    }
  }

  /**
   * Tests correctness of {@link LogSegmentName#getPosition} and
   * {@link LogSegmentName#getGeneration}.
   */
  @Test
  public void getPositionAndGenerationTest() {
    for (int i = 0; i < 10; i++) {
      long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      checkPosAndGeneration(LogSegmentName.fromPositionAndGeneration(pos, gen), pos, gen);
    }
    try {
      LogSegmentName.fromString("").getPosition();
      fail("Should have failed to get position for empty log segment name");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
    try {
      LogSegmentName.fromString("").getGeneration();
      fail("Should have failed to get generation for empty log segment name");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Tests correctness of {@link LogSegmentName#fromPositionAndGeneration(long, long)}.
   */
  @Test
  public void getNameTest() {
    for (int i = 0; i < 10; i++) {
      long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      assertEquals("Did not get expected name", pos + BlobStore.SEPARATOR + gen,
          LogSegmentName.fromPositionAndGeneration(pos, gen).toString());
    }
  }

  /**
   * Tests correctness of {@link LogSegmentName#getNextPositionName} and
   * {@link LogSegmentName#getNextGenerationName}.
   */
  @Test
  public void getNextPositionAndGenerationTest() {
    for (int i = 0; i < 10; i++) {
      long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      LogSegmentName name = LogSegmentName.fromPositionAndGeneration(pos, gen);
      checkPosAndGeneration(name.getNextPositionName(), pos + 1, 0);
      checkPosAndGeneration(name.getNextGenerationName(), pos, gen + 1);
    }
    try {
      LogSegmentName.fromString("").getNextPositionName();
      fail("Should have failed to get next position for empty log segment name");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
    try {
      LogSegmentName.fromString("").getNextGenerationName();
      fail("Should have failed to get next generation for empty log segment name");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Tests correctness of {@link LogSegmentName#generateFirstSegmentName(boolean)} for different numbers of log
   * segments (including invalid ones).
   */
  @Test
  public void generateFirstSegmentNameTest() {
    LogSegmentName singleSegmentName = LogSegmentName.generateFirstSegmentName(false);
    assertEquals("Did not get expected name", "", singleSegmentName.toString());
    assertTrue("Should be single segment name", singleSegmentName.isSingleSegment());
    LogSegmentName firstSegmentName = LogSegmentName.fromPositionAndGeneration(0, 0);
    assertEquals("Did not get expected name", firstSegmentName, LogSegmentName.generateFirstSegmentName(true));
  }

  /**
   * Tests correctness of {@link LogSegmentName#fromFilename(String)}.
   */
  @Test
  public void nameFromFilenameTest() {
    LogSegmentName singleSegmentName = LogSegmentName.fromFilename("log_current");
    assertEquals("Did not get expected name", "", singleSegmentName.toString());
    assertTrue("Should be single segment name", singleSegmentName.isSingleSegment());
    LogSegmentName name = LogSegmentName.fromPositionAndGeneration(0, 0);
    String filename = name.toFilename();
    assertEquals("Did not get expected name", name, LogSegmentName.fromFilename(filename));

    // bad file names
    String badNameBase = TestUtils.getRandomString(10);
    String[] badNames =
        {badNameBase, badNameBase + LogSegmentName.SUFFIX, name + BlobStore.SEPARATOR + "123" + LogSegmentName.SUFFIX};
    for (String badName : badNames) {
      try {
        LogSegmentName.fromFilename(badName);
        fail("Should have failed to get name for filename [" + badName + "]");
      } catch (IllegalArgumentException | StringIndexOutOfBoundsException e) {
        // expected. Nothing to do.
      }
    }
  }

  /**
   * Tests correctness of {@link LogSegmentName#toFilename}.
   */
  @Test
  public void nameToFilenameTest() {
    assertEquals("Did not get expected file name", "log_current", LogSegmentName.fromString("").toFilename());
    LogSegmentName name = LogSegmentName.fromPositionAndGeneration(3, 1);
    assertEquals("Did not get expected file name", name + LogSegmentName.SUFFIX, name.toFilename());
  }

  // helpers
  // general

  /**
   * Checks the position and generation obtained from {@code name} match {@code expectedPos} and {@code expectedGen}
   * respectively.
   * @param name the name whose position and generation needs to be checked.
   * @param expectedPos the expected return value from {@link LogSegmentName#getPosition}.
   * @param expectedGen the expected return value from {@link LogSegmentName#getGeneration}.
   */
  private void checkPosAndGeneration(LogSegmentName name, long expectedPos, long expectedGen) {
    assertEquals("Did not get expected position", expectedPos, name.getPosition());
    assertEquals("Did not get expected generation number", expectedGen, name.getGeneration());
  }

  // filenameFilterTest() helpers

  /**
   * Creates a file in {@code parentDir} with name {@code filename} and configures it to be deleted on exit.
   * @param parentDir the directory where a file with {@code filename} needs to be created.
   * @param filename the name of the file to be created.
   * @return a reference to the created {@link File}.
   * @throws IOException
   */
  private File createFile(File parentDir, String filename) throws IOException {
    File file = new File(parentDir, filename);
    if (!file.exists()) {
      assertTrue("File could not be created at path " + file.getAbsolutePath(), file.createNewFile());
    }
    file.deleteOnExit();
    return file;
  }
}
