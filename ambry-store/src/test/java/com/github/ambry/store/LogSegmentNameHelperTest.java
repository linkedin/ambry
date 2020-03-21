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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests the helper functions of {@link LogSegmentNameHelper}
 */
public class LogSegmentNameHelperTest {

  /**
   * Tests the comparator in {@link LogSegmentNameHelper} for correctness.
   */
  @Test
  public void comparatorTest() {
    Comparator<String> comparator = LogSegmentNameHelper.COMPARATOR;
    // compare empty name with empty name
    assertEquals("Empty names should be equal", 0, comparator.compare("", ""));
    // create sample names
    String[] names =
        {LogSegmentNameHelper.getName(0, 0), LogSegmentNameHelper.getName(0, 1), LogSegmentNameHelper.getName(1, 0),
            LogSegmentNameHelper.getName(1, 1)};
    for (int i = 0; i < names.length; i++) {
      for (int j = 0; j < names.length; j++) {
        int expectCompare = Integer.compare(i, j);
        assertEquals("Unexpected value on compare", expectCompare, comparator.compare(names[i], names[j]));
        assertEquals("Unexpected value on compare", -1 * expectCompare, comparator.compare(names[j], names[i]));
      }
    }
    // empty name cannot be compared with anything else
    String validName = LogSegmentNameHelper.getName(0, 0);
    try {
      comparator.compare(validName, "");
      fail("Should not have been able to compare empty name with anything else");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
    try {
      comparator.compare("", validName);
      fail("Should not have been able to compare empty name with anything else");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Checks the file name filter in {@link LogSegmentNameHelper} for correctness by creating valid and invalid files
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
      String filename = LogSegmentNameHelper.nameToFilename("");
      File file = createFile(tempDir, filename);
      validFiles.add(file);
      for (int i = 1; i < validFileCount; i++) {
        long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
        long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
        filename = LogSegmentNameHelper.nameToFilename(LogSegmentNameHelper.getName(pos, gen));
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
            filename = filename + LogSegmentNameHelper.SUFFIX + "_temp";
            break;
          default:
            break;
        }
        createFile(tempDir, filename);
      }
      Set<File> filteredFiles = new HashSet<>(Arrays.asList(tempDir.listFiles(LogSegmentNameHelper.LOG_FILE_FILTER)));
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
   * Tests correctness of {@link LogSegmentNameHelper#hashcode(String)}
   */
  @Test
  public void hashCodeTest() {
    String name = TestUtils.getRandomString(10);
    assertEquals("Hashcode is not as expected", name.hashCode(), LogSegmentNameHelper.hashcode(name));
  }

  /**
   * Tests correctness of {@link LogSegmentNameHelper#getPosition(String)} and
   * {@link LogSegmentNameHelper#getGeneration(String)}.
   */
  @Test
  public void getPositionAndGenerationTest() {
    for (int i = 0; i < 10; i++) {
      long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      checkPosAndGeneration(LogSegmentNameHelper.getName(pos, gen), pos, gen);
    }
    try {
      LogSegmentNameHelper.getPosition("");
      fail("Should have failed to get position for empty log segment name");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
    try {
      LogSegmentNameHelper.getGeneration("");
      fail("Should have failed to get generation for empty log segment name");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Tests correctness of {@link LogSegmentNameHelper#getName(long, long)}.
   */
  @Test
  public void getNameTest() {
    for (int i = 0; i < 10; i++) {
      long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      assertEquals("Did not get expected name", pos + BlobStore.SEPARATOR + gen,
          LogSegmentNameHelper.getName(pos, gen));
    }
  }

  /**
   * Tests correctness of {@link LogSegmentNameHelper#getNextPositionName(String)} and
   * {@link LogSegmentNameHelper#getNextGenerationName(String)}.
   */
  @Test
  public void getNextPositionAndGenerationTest() {
    for (int i = 0; i < 10; i++) {
      long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
      String name = LogSegmentNameHelper.getName(pos, gen);
      checkPosAndGeneration(LogSegmentNameHelper.getNextPositionName(name), pos + 1, 0);
      checkPosAndGeneration(LogSegmentNameHelper.getNextGenerationName(name), pos, gen + 1);
    }
    try {
      LogSegmentNameHelper.getNextPositionName("");
      fail("Should have failed to get next position for empty log segment name");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
    try {
      LogSegmentNameHelper.getNextGenerationName("");
      fail("Should have failed to get next generation for empty log segment name");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Tests correctness of {@link LogSegmentNameHelper#generateFirstSegmentName(boolean)} for different numbers of log
   * segments (including invalid ones).
   */
  @Test
  public void generateFirstSegmentNameTest() {
    assertEquals("Did not get expected name", "", LogSegmentNameHelper.generateFirstSegmentName(false));
    String firstSegmentName = LogSegmentNameHelper.getName(0, 0);
    assertEquals("Did not get expected name", firstSegmentName, LogSegmentNameHelper.generateFirstSegmentName(true));
  }

  /**
   * Tests correctness of {@link LogSegmentNameHelper#nameFromFilename(String)}.
   */
  @Test
  public void nameFromFilenameTest() {
    assertEquals("Did not get expected name", "", LogSegmentNameHelper.nameFromFilename("log_current"));
    String name = LogSegmentNameHelper.getName(0, 0);
    String filename = LogSegmentNameHelper.nameToFilename(name);
    assertEquals("Did not get expected name", name, LogSegmentNameHelper.nameFromFilename(filename));

    // bad file names
    String badNameBase = TestUtils.getRandomString(10);
    String[] badNames = {badNameBase, badNameBase + LogSegmentNameHelper.SUFFIX,
        name + BlobStore.SEPARATOR + "123" + LogSegmentNameHelper.SUFFIX};
    for (String badName : badNames) {
      try {
        LogSegmentNameHelper.nameFromFilename(badName);
        fail("Should have failed to get name for filename [" + badName + "]");
      } catch (IllegalArgumentException | StringIndexOutOfBoundsException e) {
        // expected. Nothing to do.
      }
    }
  }

  /**
   * Tests correctness of {@link LogSegmentNameHelper#nameToFilename(String)}.
   */
  @Test
  public void nameToFilenameTest() {
    assertEquals("Did not get expected file name", "log_current", LogSegmentNameHelper.nameToFilename(""));
    String name = TestUtils.getRandomString(10);
    assertEquals("Did not get expected file name", name + LogSegmentNameHelper.SUFFIX,
        LogSegmentNameHelper.nameToFilename(name));
  }

  // helpers
  // general

  /**
   * Checks the position and generation obtained from {@code name} match {@code expectedPos} and {@code expectedGen}
   * respectively.
   * @param name the name whose position and generation needs to be checked.
   * @param expectedPos the expected return value from {@link LogSegmentNameHelper#getPosition(String)}.
   * @param expectedGen the expected return value from {@link LogSegmentNameHelper#getGeneration(String)}.
   */
  private void checkPosAndGeneration(String name, long expectedPos, long expectedGen) {
    assertEquals("Did not get expected position", expectedPos, LogSegmentNameHelper.getPosition(name));
    assertEquals("Did not get expected generation number", expectedGen, LogSegmentNameHelper.getGeneration(name));
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
