/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests {@link DiskSpaceAllocator} for expected behavior.
 */
public class DiskSpaceAllocatorTest {
  private static final StorageManagerMetrics METRICS = new StorageManagerMetrics(new MetricRegistry());
  private int requiredSwapSegmentsPerSize = 0;
  private final File allocatedFileDir;
  private final File reserveFileDir;
  private final String storeId0 = "0";
  private final String storeId1 = "1";
  private final String storeId2 = "2";
  private DiskSpaceAllocator alloc;
  private ExecutorService exec;

  public DiskSpaceAllocatorTest() throws Exception {
    File tempDir = StoreTestUtils.createTempDirectory("disk-space-allocator-test");
    reserveFileDir = new File(tempDir, "reserve-files");
    allocatedFileDir = new File(tempDir, "allocated-files");
    if (!allocatedFileDir.mkdir()) {
      throw new IOException("Could not create directory for allocated files");
    }
  }

  @After
  public void after() throws Exception {
    if (exec != null) {
      exec.shutdownNow();
    }
  }

  /**
   * Test behavior when segments are allocated before the pool is fully initialized.
   * @throws Exception
   */
  @Test
  public void allocateBeforeInitializeTest() throws Exception {
    // For general testing purpose, following tests assume store may have different segment sizes.
    alloc = constructAllocator();
    File f1 = allocateAndVerify(storeId1, "file1", 50, false);
    File f2 = allocateAndVerify(storeId1, "file2", 20, false);
    File f3 = allocateAndVerify(storeId1, "file3", 20, false);
    // free one file before initializing pool
    freeAndVerify(storeId1, f3, 20, false);
    // expect the pool to still be empty
    verifyPoolState(new ExpectedState());

    alloc.initializePool(
        Arrays.asList(new DiskSpaceRequirements(storeId0, 50, 2, 0), new DiskSpaceRequirements(storeId1, 21, 1, 0)));
    // return files that were allocated before initialization to the pool
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 2).addStoreSeg(storeId1, 21, 1));
    freeAndVerify(storeId1, f1, 50, false);
    verifyPoolState(
        new ExpectedState().addStoreSeg(storeId0, 50, 2).addStoreSeg(storeId1, 21, 1).addStoreSeg(storeId1, 50, 1));
    freeAndVerify(storeId1, f2, 20, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 2)
        .addStoreSeg(storeId1, 20, 1)
        .addStoreSeg(storeId1, 21, 1)
        .addStoreSeg(storeId1, 50, 1));
    // allocate and free file from initialized pool
    File f4 = allocateAndVerify(storeId1, "file4", 50, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 2)
        .addStoreSeg(storeId1, 20, 1)
        .addStoreSeg(storeId1, 21, 1)
        .addStoreSeg(storeId1, 50, 0));
    freeAndVerify(storeId1, f4, 50, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 2)
        .addStoreSeg(storeId1, 20, 1)
        .addStoreSeg(storeId1, 21, 1)
        .addStoreSeg(storeId1, 50, 1));
  }

  /**
   * Test a large number of concurrent alloc/free operations
   * @throws Exception
   */
  @Test
  public void concurrencyTest() throws Exception {
    alloc = constructAllocator();
    List<DiskSpaceRequirements> requirementsList = new ArrayList<>();
    requirementsList.add(new DiskSpaceRequirements(storeId0, 10, 500, 1));
    requirementsList.add(new DiskSpaceRequirements(storeId1, 5, 251, 0));
    alloc.initializePool(requirementsList);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 10, 500).addStoreSeg(storeId1, 5, 251));
    exec = Executors.newCachedThreadPool();
    // allocate all files in pool
    runConcurrencyTest(requirementsList, true, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 10, 0).addStoreSeg(storeId1, 5, 0));
    // free all files from last run
    runConcurrencyTest(requirementsList, false, true);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 10, 500).addStoreSeg(storeId1, 5, 251));
    // allocate and free all files
    runConcurrencyTest(requirementsList, true, true);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 10, 500).addStoreSeg(storeId1, 5, 251));
    // allocate all files
    runConcurrencyTest(requirementsList, true, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 10, 0).addStoreSeg(storeId1, 5, 0));
  }

  /**
   * Test allocation of file sizes that are not in the pool after the pool has been initialized.
   * @throws Exception
   */
  @Test
  public void sizeNotInPoolTest() throws Exception {
    alloc = constructAllocator();
    alloc.initializePool(
        Arrays.asList(new DiskSpaceRequirements(storeId0, 50, 2, 0), new DiskSpaceRequirements(storeId1, 21, 1, 0)));
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 2).addStoreSeg(storeId1, 21, 1));
    File f1 = allocateAndVerify(storeId0, "file1", 25, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 2).addStoreSeg(storeId1, 21, 1));
    freeAndVerify(storeId0, f1, 25, false);
    verifyPoolState(
        new ExpectedState().addStoreSeg(storeId0, 25, 1).addStoreSeg(storeId0, 50, 2).addStoreSeg(storeId1, 21, 1));
    // try checking out same file again
    File f2 = allocateAndVerify(storeId0, "file2", 25, false);
    verifyPoolState(
        new ExpectedState().addStoreSeg(storeId0, 25, 0).addStoreSeg(storeId0, 50, 2).addStoreSeg(storeId1, 21, 1));
    freeAndVerify(storeId0, f2, 25, false);
    verifyPoolState(
        new ExpectedState().addStoreSeg(storeId0, 25, 1).addStoreSeg(storeId0, 50, 2).addStoreSeg(storeId1, 21, 1));
  }

  /**
   * This tests various cases where the allocator is restarted and disk space requirements change.
   * @throws Exception
   */
  @Test
  public void subsequentStartupTest() throws Exception {
    requiredSwapSegmentsPerSize = 1;
    alloc = constructAllocator();
    List<DiskSpaceRequirements> requirementsList = new ArrayList<>();

    requirementsList.add(new DiskSpaceRequirements(storeId0, 5, 6, 0));
    requirementsList.add(new DiskSpaceRequirements(storeId1, 3, 7, 0));
    requirementsList.add(new DiskSpaceRequirements(storeId2, 5, 4, 0));
    alloc.initializePool(requirementsList);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 5, 6)
        .addStoreSeg(storeId1, 3, 7)
        .addStoreSeg(storeId2, 5, 4)
        .addSwapSeg(5, 1)
        .addSwapSeg(3, 1));
    File f1 = allocateAndVerify(storeId0, "file1", 5, false);
    File f2 = allocateAndVerify(storeId1, "file2", 3, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 5, 5)
        .addStoreSeg(storeId1, 3, 6)
        .addStoreSeg(storeId2, 5, 4)
        .addSwapSeg(5, 1)
        .addSwapSeg(3, 1));
    // second startup, test freeing old files after initialization
    alloc = constructAllocator();
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 5, 5)
        .addStoreSeg(storeId1, 3, 6)
        .addStoreSeg(storeId2, 5, 4)
        .addSwapSeg(5, 1)
        .addSwapSeg(3, 1));
    alloc.initializePool(requirementsList);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 5, 6)
        .addStoreSeg(storeId1, 3, 7)
        .addStoreSeg(storeId2, 5, 4)
        .addSwapSeg(5, 1)
        .addSwapSeg(3, 1));
    freeAndVerify(storeId0, f1, 5, false);
    freeAndVerify(storeId1, f2, 3, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 5, 7)
        .addStoreSeg(storeId1, 3, 8)
        .addStoreSeg(storeId2, 5, 4)
        .addSwapSeg(5, 1)
        .addSwapSeg(3, 1));
    // third startup, test allocating files before initialization and changing disk space requirements when initializing
    alloc = constructAllocator();
    f1 = allocateAndVerify(storeId1, "file1", 3, false);
    f2 = allocateAndVerify(storeId2, "file2", 5, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 5, 7)
        .addStoreSeg(storeId1, 3, 7)
        .addStoreSeg(storeId2, 5, 3)
        .addSwapSeg(5, 1)
        .addSwapSeg(3, 1));
    // files freed before init should be discarded, not returned to the pool
    freeAndVerify(storeId1, f1, 3, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 5, 7)
        .addStoreSeg(storeId1, 3, 7)
        .addStoreSeg(storeId2, 5, 3)
        .addSwapSeg(5, 1)
        .addSwapSeg(3, 1));
    requirementsList.clear();
    requirementsList.add(new DiskSpaceRequirements(storeId0, 3, 20, 1));
    requirementsList.add(new DiskSpaceRequirements(storeId1, 5, 6, 1));
    requirementsList.add(new DiskSpaceRequirements(storeId2, 6, 1, 1));
    alloc.initializePool(requirementsList);
    verifyPoolState(
        new ExpectedState().addStoreSeg(storeId0, 3, 20).addStoreSeg(storeId1, 5, 6).addStoreSeg(storeId2, 6, 1));
    freeAndVerify(storeId2, f2, 5, false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 3, 20)
        .addStoreSeg(storeId1, 5, 6)
        .addStoreSeg(storeId2, 6, 1)
        .addStoreSeg(storeId2, 5, 1));
  }

  /**
   * Test various swap segment usage combinations
   * @throws Exception
   */
  @Test
  public void swapSegmentRequirementTest() throws Exception {
    requiredSwapSegmentsPerSize = 4;
    alloc = constructAllocator();
    List<DiskSpaceRequirements> requirementsList = new ArrayList<>();
    requirementsList.add(new DiskSpaceRequirements(storeId0, 3, 7, 0));
    requirementsList.add(new DiskSpaceRequirements(storeId1, 5, 4, 2));
    // This should result in 0 swap segments b/c 0 > 4 - 10
    requirementsList.add(new DiskSpaceRequirements(storeId2, 7, 3, 10));
    Collections.shuffle(requirementsList);
    alloc.initializePool(requirementsList);
    verifyPoolState(
        new ExpectedState().addStoreSeg(storeId0, 3, 7).addStoreSeg(storeId1, 5, 4).addStoreSeg(storeId2, 7, 3).
            addSwapSeg(3, 4).addSwapSeg(5, 2));

    // test reinitialization with different requirements, and num swap used for each size
    requiredSwapSegmentsPerSize = 3;
    alloc = constructAllocator();
    requirementsList.clear();
    requirementsList.add(new DiskSpaceRequirements(storeId0, 3, 2, 0));
    requirementsList.add(new DiskSpaceRequirements(storeId1, 5, 4, 2));
    requirementsList.add(new DiskSpaceRequirements(storeId2, 7, 3, 1));
    Collections.shuffle(requirementsList);
    alloc.initializePool(requirementsList);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 3, 2)
        .addStoreSeg(storeId1, 5, 4)
        .addStoreSeg(storeId2, 7, 3)
        .addSwapSeg(3, 3)
        .addSwapSeg(5, 1)
        .addSwapSeg(7, 2));
  }

  /**
   * This tests the case where we cannot create a reserve file directory, because a normal file at that path already
   * exists.
   * @throws Exception
   */
  @Test
  public void invalidReserveFileDirTest() throws Exception {
    assertTrue("Could not create file", reserveFileDir.createNewFile());
    alloc = constructAllocator();
    // Should still allow allocation of non-pooled files before init.
    File f1 = allocateAndVerify(storeId0, "file1", 5, false);
    freeAndVerify(storeId0, f1, 5, false);
    assertFalse("Reserve file dir should not have been created if a file already exists at that path",
        reserveFileDir.isDirectory());
    try {
      alloc.initializePool(Collections.emptyList());
      fail("Expected StoreException");
    } catch (StoreException e) {
      assertEquals("Wrong error code", StoreErrorCodes.Initialization_Error, e.getErrorCode());
    }
    // Should still allow allocation of non-pooled files even after init failure.
    File f2 = allocateAndVerify(storeId0, "file1", 5, false);
    freeAndVerify(storeId0, f2, 5, false);
    assertFalse("Reserve file dir should not have been created if a file already exists at that path",
        reserveFileDir.isDirectory());
  }

  /**
   * Test that {@link DiskSpaceAllocator#inventoryExistingReserveFiles()} can correctly delete invalid directories/files
   * and inventory valid ones.
   * @throws Exception
   */
  @Test
  public void inventoryExistingFilesAndInitTest() throws Exception {
    assertTrue("Couldn't create reserve file directory", reserveFileDir.mkdir());
    // create valid existing store reserve directory and swap directory
    File storeReserveDir = new File(reserveFileDir, DiskSpaceAllocator.STORE_DIR_PREFIX + storeId0);
    File swapReserveDir = new File(reserveFileDir, DiskSpaceAllocator.SWAP_DIR_NAME);
    assertTrue("Couldn't create store reserve directory", storeReserveDir.mkdir());
    assertTrue("Couldn't create swap reserve directory", swapReserveDir.mkdir());
    // create sub-directories in store and swap reserve directories
    File fileSizeDir1 = new File(storeReserveDir, DiskSpaceAllocator.FILE_SIZE_DIR_PREFIX + 50);
    File fileSizeDir2 = new File(swapReserveDir, DiskSpaceAllocator.FILE_SIZE_DIR_PREFIX + 30);
    File fileSizeDir3 = new File(swapReserveDir, DiskSpaceAllocator.FILE_SIZE_DIR_PREFIX + 50);
    assertTrue("Couldn't create file size directory", fileSizeDir1.mkdir());
    assertTrue("Couldn't create file size directory", fileSizeDir2.mkdir());
    assertTrue("Couldn't create file size directory", fileSizeDir3.mkdir());
    // create files in above directories
    File reserveFile1 = new File(fileSizeDir1, DiskSpaceAllocator.RESERVE_FILE_PREFIX + UUID.randomUUID());
    File reserveFile2 = new File(fileSizeDir2, DiskSpaceAllocator.RESERVE_FILE_PREFIX + UUID.randomUUID());
    File reserveFile3 = new File(fileSizeDir3, DiskSpaceAllocator.RESERVE_FILE_PREFIX + UUID.randomUUID());
    assertTrue("Couldn't create file", reserveFile1.createNewFile());
    assertTrue("Couldn't create file", reserveFile2.createNewFile());
    assertTrue("Couldn't create file", reserveFile3.createNewFile());
    // create invalid directory and file for testing
    File invalidDir = new File(reserveFileDir, DiskSpaceAllocator.FILE_SIZE_DIR_PREFIX + 50);
    File invalidFile = new File(reserveFileDir, DiskSpaceAllocator.RESERVE_FILE_PREFIX + UUID.randomUUID());
    assertTrue(invalidDir.mkdir());
    assertTrue(invalidFile.createNewFile());
    // create extra file inside invalid directory to mock non-empty directory
    File extraFile = new File(invalidDir, DiskSpaceAllocator.RESERVE_FILE_PREFIX + UUID.randomUUID());
    assertTrue(extraFile.createNewFile());
    // create an extra store reserve dir (storeId1) to test if DSA can correctly delete the unrecognized dir
    File extraStoreReserveDir = new File(reserveFileDir, DiskSpaceAllocator.STORE_DIR_PREFIX + storeId1);
    assertTrue("Couldn't create an extra store reserve directory", extraStoreReserveDir.mkdir());
    // create size dir and put some files into this extra store reserve dir
    File sizeDir = new File(extraStoreReserveDir, DiskSpaceAllocator.FILE_SIZE_DIR_PREFIX + 50);
    assertTrue("Couldn't create file size directory in extra store reserve dir", sizeDir.mkdir());
    File randomFile = new File(sizeDir, DiskSpaceAllocator.RESERVE_FILE_PREFIX + UUID.randomUUID());
    assertTrue("Couldn't create random file in extra store reserve dir", randomFile.createNewFile());
    // instantiate DiskSpaceAllocator
    alloc = constructAllocator();
    // verify invalid directories and files no longer exist
    assertFalse("Invalid directory shouldn't exist after inventory", invalidDir.exists());
    assertFalse("Invalid file shouldn't exist after inventory", invalidFile.exists());
    // verify inventoried in-mem store/swap reserve file maps match those existing valid files
    Map<String, DiskSpaceAllocator.ReserveFileMap> storeFileMap = alloc.getStoreReserveFileMap();
    // note that, after inventory, store reserve file map contains extra storeId1, which will be deleted when initializePool is called.
    assertTrue("Store reserve file map doesn't match existing directory/file",
        storeFileMap.containsKey(storeId0) && storeFileMap.containsKey(storeId1) && storeFileMap.size() == 2);
    DiskSpaceAllocator.ReserveFileMap swapFileMap = alloc.getSwapReserveFileMap();
    assertTrue("Swap reserve file map doesn't match existing directory/file",
        swapFileMap.getCount(30) == 1 && swapFileMap.getFileSizeSet().size() == 2);
    assertTrue("Swap reserve file map doesn't match existing directory/file",
        swapFileMap.getCount(50) == 1 && swapFileMap.getFileSizeSet().size() == 2);

    // initialize pool to verify extra swap segment is deleted and required store segments are added.
    // before init: store reserve pool has reserve_50 segment; swap reserve pool has reserve_30, reserve_50
    // after init (expected state): store reserve pool has reserve_30 segment; swap reserve pool is empty.
    alloc.initializePool(Collections.singletonList(new DiskSpaceRequirements(storeId0, 30, 1, 1)));
    swapFileMap = alloc.getSwapReserveFileMap();
    assertNull("The swap segment should not exist in in-mem swap map", swapFileMap.remove(30));
    assertNull("The swap segment should not exist in in-mem swap map", swapFileMap.remove(50));
    storeFileMap = alloc.getStoreReserveFileMap();
    assertTrue("Store reserve file map should not contain storeId1 dir",
        !storeFileMap.containsKey(storeId1) && storeFileMap.size() == 1);
    assertNull("The store segment with size 50 should not exist in in-mem store file map",
        storeFileMap.get(storeId0).remove(50));
    assertNotNull("The store segment with size 30 should exist", storeFileMap.get(storeId0).remove(30));
  }

  /**
   * Exercises different failure cases for {@link DiskSpaceAllocator#initializePool(Collection)}.
   * @throws Exception
   */
  @Test
  public void initFailureTest() throws Exception {
    // build a pool with files that we can modify permissions on to induce failures
    alloc = constructAllocator();
    alloc.initializePool(Collections.singletonList(new DiskSpaceRequirements(storeId0, 50, 2, 0)));
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 2));

    File storeReserveDir = new File(reserveFileDir, DiskSpaceAllocator.STORE_DIR_PREFIX + storeId0);
    File fileSizeDir = new File(storeReserveDir, DiskSpaceAllocator.generateFileSizeDirName(50));

    // test a failure while deleting an unneeded directory
    runInitFailureTest(fileSizeDir, false);
    // test a failure while deleting an unneeded individual file
    runInitFailureTest(fileSizeDir, false, new DiskSpaceRequirements(storeId0, 50, 1, 0));
    // test that an inventory failure during DSA construction results in an exception thrown by initializePool
    runInitFailureTest(reserveFileDir, true);
  }

  /**
   * Test situations where the allocator should throw exceptions during allocate and free calls.
   * @throws Exception
   */
  @Test
  public void allocateAndFreeFailureTest() throws Exception {
    alloc = constructAllocator();
    alloc.initializePool(Collections.singletonList(new DiskSpaceRequirements(storeId0, 50, 1, 0)));
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 1));
    // test when a destination file already exists
    File f1 = new File(allocatedFileDir, "f1");
    assertTrue("Could not create file", f1.createNewFile());
    TestUtils.assertException(IOException.class, () -> alloc.allocate(f1, 50, storeId0, false), null);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 1));
    // test returning a file that does not exist
    File f2 = new File(allocatedFileDir, "f2");
    TestUtils.assertException(IOException.class, () -> alloc.free(f2, 50, storeId0, false), null);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 1));
  }

  /**
   * Test the allocator with pooling disabled. The reserve file pool should never be created
   */
  @Test
  public void unpooledAllocatorTest() throws Exception {
    alloc = constructUnpooledAllocator();
    verifyPoolState(null);
    File f1 = allocateAndVerify(storeId0, "file1", 20, false);
    verifyPoolState(null);
    freeAndVerify(storeId0, f1, 20, false);
    verifyPoolState(null);

    // initializing should be a no-op
    alloc.initializePool(Collections.singletonList(new DiskSpaceRequirements(storeId0, 20, 1, 0)));
    verifyPoolState(null);
    File f2 = allocateAndVerify(storeId0, "file2", 20, false);
    verifyPoolState(null);
    freeAndVerify(storeId0, f2, 20, false);
    verifyPoolState(null);
  }

  /**
   * Test that disk allocator is able to dynamically add/delete segments for certain store.
   * @throws Exception
   */
  @Test
  public void addAndDeleteSegmentsTest() throws Exception {
    alloc = constructAllocator();
    alloc.initializePool(Collections.singletonList(new DiskSpaceRequirements(storeId0, 50, 1, 0)));
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 1));
    // add requirements for new store (storeId1)
    DiskSpaceRequirements requirements1 = new DiskSpaceRequirements(storeId1, 20L, 3L, 1L);
    alloc.addRequiredSegments(alloc.getOverallRequirements(Collections.singletonList(requirements1)), false);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 1).addStoreSeg(storeId1, 20, 3));
    // verify that swapSegmentNumBySize map has two entries and there is one segment for size 20L
    assertTrue("The number of swap segments in map is not expected",
        alloc.getSwapSegmentBySizeMap().size() == 2 && alloc.getSwapSegmentBySizeMap().get(20L) == 1L);
    // test that segments associated with storeId0 can be correctly deleted
    alloc.deleteAllSegmentsForStoreIds(Collections.singletonList(storeId0));
    verifyPoolState(new ExpectedState().addStoreSeg(storeId1, 20, 3));
    // verify in-mem store reserve file map doesn't contain storeId0
    Map<String, DiskSpaceAllocator.ReserveFileMap> storeFileMap = alloc.getStoreReserveFileMap();
    assertFalse("Store reserve file map should not contain entry associated with storeId0",
        storeFileMap.containsKey(storeId0));
  }

  /**
   * Test the allocator with pooling disabled. Add or delete segments for certain store should be no-op.
   * @throws Exception
   */
  @Test
  public void addAndDeleteWithUnpooledAllocatorTest() throws Exception {
    alloc = constructUnpooledAllocator();
    verifyPoolState(null);
    // test that add segments should be no-op
    Map<String, Map<Long, Long>> requirements = new HashMap<>();
    Map<Long, Long> sizeToFileNum = new HashMap<>();
    sizeToFileNum.put(20L, 4L);
    sizeToFileNum.put(50L, 1L);
    requirements.put(storeId1, sizeToFileNum);
    alloc.addRequiredSegments(requirements, true);
    verifyPoolState(null);
    // test that delete segments should be no-op
    alloc.deleteAllSegmentsForStoreIds(Collections.singletonList(storeId1));
    verifyPoolState(null);
  }

  /**
   * Test that failure occurs when adding required segments for new store. SwapSegmentBySize map is supposed to reduce
   * number of swap segments (including both swap segments in use and those available in pool)
   * @throws Exception
   */
  @Test
  public void addRequiredSegmentsFailureTest() throws Exception {
    requiredSwapSegmentsPerSize = 2;
    alloc = constructAllocator();
    alloc.initializePool(Collections.singletonList(new DiskSpaceRequirements(storeId0, 50, 1, 0)));
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 50, 1).addSwapSeg(50, 2));
    assertEquals("Mismatch in number of swap segments", 2L, (long) alloc.getSwapSegmentBySizeMap().get(50L));
    // create disk space requirements for store1
    DiskSpaceRequirements requirements1 = new DiskSpaceRequirements(storeId1, 20L, 3L, 1L);
    Map<String, Map<Long, Long>> overallRequirements =
        alloc.getOverallRequirements(Collections.singletonList(requirements1));
    assertEquals("Mismatch in number of swap segments", 2L, (long) alloc.getSwapSegmentBySizeMap().get(20L));

    File swapReserveDir = new File(reserveFileDir, DiskSpaceAllocator.SWAP_DIR_NAME);
    File swapSegSizeDir = new File(swapReserveDir, DiskSpaceAllocator.FILE_SIZE_DIR_PREFIX + 20);
    // create a file there to trigger add segments failure
    assertTrue("Cannot create file", swapSegSizeDir.createNewFile());
    try {
      alloc.addRequiredSegments(overallRequirements, false);
      fail("should fail");
    } catch (IOException e) {
      //expected
    }
    assertEquals("Mismatch in number of swap segments", 1L, (long) alloc.getSwapSegmentBySizeMap().get(20L));
  }

  /**
   * Test the case where swap segments are allocated and freed.
   * @throws Exception
   */
  @Test
  public void allocateAndFreeSwapSegmentTest() throws Exception {
    requiredSwapSegmentsPerSize = 2;
    alloc = constructAllocator();
    alloc.initializePool(Collections.singletonList(new DiskSpaceRequirements(storeId0, 20, 3, 0)));
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 20, 3).addSwapSeg(20, 2));
    File f1 = allocateAndVerify(storeId0, "file1", 20, true);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 20, 3).addSwapSeg(20, 1));
    freeAndVerify(storeId0, f1, 20, true);
    verifyPoolState(new ExpectedState().addStoreSeg(storeId0, 20, 3).addSwapSeg(20, 2));
  }

  /**
   * Allocate a file and check for existence and write permissions.
   * @param storeId the id of store for which is file is allocated.
   * @param filename the name of the destination file. This file will be created in the allocated file directory.
   * @param size the size of the file to allocate.
   * @param isSwapSegment whether this is a swap segment to allocate.
   * @return a {@link File} object for the allocated file.
   * @throws Exception
   */
  private File allocateAndVerify(String storeId, String filename, long size, boolean isSwapSegment) throws Exception {
    File file = new File(allocatedFileDir, filename);
    alloc.allocate(file, size, storeId, isSwapSegment);
    assertTrue("Allocated file should exist: " + file.getAbsolutePath(), file.exists());
    assertTrue("Allocated file should be a file and not a directory: " + file.getAbsolutePath(), file.isFile());
    assertTrue("Allocated file should be writable: " + file.getAbsolutePath(), file.canWrite());
    return file;
  }

  /**
   * Free a file and check that it was moved away from the allocated file's path.
   * @param storeId the id of store which the file is associated with.
   * @param file the file to free.
   * @param size the size of the file (required by the API of {@link DiskSpaceAllocator#free(File, long, String, boolean)}
   * @param isSwapSegment whether this is a swap segment to free.
   * @throws Exception
   */
  private void freeAndVerify(String storeId, File file, long size, boolean isSwapSegment) throws Exception {
    alloc.free(file, size, storeId, isSwapSegment);
    assertFalse("File should have been returned to the pool: " + file.getAbsolutePath(), file.exists());
  }

  /**
   * Run a concurrency test with multiple parallel tasks, each one allocating and/or freeing a file called
   * "conc-test-{n}"
   * @param requirementsList The list of {@link DiskSpaceRequirements} used to determine the number of operations to
   *                         start
   * @param allocate {@code true} if each task should allocate a new segment.
   * @param free {@code true} if each task should free a (hopefully) existing segment.
   */
  private void runConcurrencyTest(List<DiskSpaceRequirements> requirementsList, final boolean allocate,
      final boolean free) throws Exception {
    List<Callable<Void>> tasks = new ArrayList<>();
    final String filenamePrefix = "conc-test-";
    int fileCount = 0;
    for (final DiskSpaceRequirements requirements : requirementsList) {
      final AtomicReference<String> storeId = new AtomicReference<String>(requirements.getStoreId());
      for (int i = 0; i < requirements.getSegmentsNeeded(); i++, fileCount++) {
        final String filename = filenamePrefix + fileCount;
        tasks.add(() -> {
          if (allocate) {
            allocateAndVerify(storeId.get(), filename, requirements.getSegmentSizeInBytes(), false);
          }
          if (free) {
            freeAndVerify(storeId.get(), new File(allocatedFileDir, filename), requirements.getSegmentSizeInBytes(),
                false);
          }
          return null;
        });
      }
    }
    for (Future<Void> future : exec.invokeAll(tasks)) {
      future.get(10, TimeUnit.SECONDS);
    }
  }

  /**
   * Assert that an initialization error occurs when permissions on a reserve directory are modified.
   * @param directoryToRestrict the directory to make unreadable or unwritable.
   * @param restrictRead {@code true} to make the directory unreadable, or {@code false} to make it unwritable
   * @param requirements the {@link DiskSpaceRequirements} to provide to
   *                     {@link DiskSpaceAllocator#initializePool(Collection)}
   */
  private void runInitFailureTest(File directoryToRestrict, boolean restrictRead,
      DiskSpaceRequirements... requirements) {
    if (restrictRead) {
      assertTrue("Could not make unreadable", directoryToRestrict.setReadable(false));
    } else {
      assertTrue("Could not make unwritable", directoryToRestrict.setWritable(false));
    }
    alloc = constructAllocator();
    try {
      alloc.initializePool(Arrays.asList(requirements));
      fail("Expected StoreException");
    } catch (StoreException e) {
      assertEquals("Wrong error code", StoreErrorCodes.Initialization_Error, e.getErrorCode());
    } finally {
      if (restrictRead) {
        assertTrue("Could not make readable again", directoryToRestrict.setReadable(true));
      } else {
        assertTrue("Could not make writable again", directoryToRestrict.setWritable(true));
      }
    }
  }

  /**
   * Verify that the layout of the pool matches the expected state.
   * @param expectedState an {@link ExpectedState} object that describes the expected number of reserve files for each
   *                      size, or {@code null} if the directory should not exist.
   */
  private void verifyPoolState(ExpectedState expectedState) {
    verifyPoolState(reserveFileDir, expectedState);
  }

  /**
   * @return a new {@link DiskSpaceAllocator} instance.
   */
  private DiskSpaceAllocator constructAllocator() {
    return new DiskSpaceAllocator(true, reserveFileDir, requiredSwapSegmentsPerSize, METRICS);
  }

  /**
   * @return a new {@link DiskSpaceAllocator} with pooling disabled.
   */
  private DiskSpaceAllocator constructUnpooledAllocator() {
    return new DiskSpaceAllocator(false, reserveFileDir, requiredSwapSegmentsPerSize, METRICS);
  }

  /**
   * Verify that the layout of the pool matches the expected state.
   * @param reserveFileDir the reserve file directory to inspect.
   * @param expectedState an {@link ExpectedState} object that describes the expected number of reserve files for each
   *                      size, or {@code null} if the directory should not exist.
   */
  static void verifyPoolState(File reserveFileDir, ExpectedState expectedState) {
    if (expectedState == null) {
      assertFalse("Reserve directory should not exist", reserveFileDir.exists());
    } else {
      assertEquals("Wrong number of dirs", expectedState.storeReserveMap.size() + 1, reserveFileDir.list().length);
      File swapFileDir = new File(reserveFileDir, DiskSpaceAllocator.SWAP_DIR_NAME);
      assertEquals("Wrong number of dirs in swap reserve pool", expectedState.swapMap.size(),
          swapFileDir.list().length);
      for (Map.Entry<Long, Integer> entry : expectedState.swapMap.entrySet()) {
        long size = entry.getKey();
        int count = entry.getValue();
        File fileSizeDir = new File(swapFileDir, DiskSpaceAllocator.generateFileSizeDirName(size));
        String[] filenameList = fileSizeDir.list();
        assertNotNull("Error while listing swap files for size " + size, filenameList);
        assertEquals("Wrong number of swap files for size " + size, count, filenameList.length);
      }
      for (Map.Entry<String, Map<Long, Integer>> entry : expectedState.storeReserveMap.entrySet()) {
        File storeReserveDir = new File(reserveFileDir, DiskSpaceAllocator.STORE_DIR_PREFIX + entry.getKey());
        for (Map.Entry<Long, Integer> sizeToFileNum : entry.getValue().entrySet()) {
          long fileSize = sizeToFileNum.getKey();
          int fileNum = sizeToFileNum.getValue();
          File fileSizeDir = new File(storeReserveDir, DiskSpaceAllocator.generateFileSizeDirName(fileSize));
          String[] filenameList = fileSizeDir.list();
          assertNotNull("Error while listing store " + entry.getKey() + " reserve files for size " + fileSize,
              filenameList);
          assertEquals("Wrong number of store " + entry.getKey() + " reserve files for size " + fileSize, fileNum,
              filenameList.length);
        }
      }
    }
  }

  /**
   * Contains a swapMap and storeReserveMap that describes the number of reserve files for each file size and each store.
   * Used with {@link #verifyPoolState}
   */
  static class ExpectedState {
    final Map<Long, Integer> swapMap = new HashMap<>();
    final Map<String, Map<Long, Integer>> storeReserveMap = new HashMap<>();

    /**
     * Update the swapMap with an entry for a certain file size.
     * @param size the file size.
     * @param count the expected number of files
     * @return {@code this}
     */
    ExpectedState addSwapSeg(long size, int count) {
      swapMap.put(size, count);
      return this;
    }

    /**
     * Update the storeReserveMap with an entry for a certain store and certain file size.
     * @param storeId the id of store which the segment file is associated with.
     * @param size the file size
     * @param count the expected number of files.
     * @return {@code this}
     */
    ExpectedState addStoreSeg(String storeId, long size, int count) {
      Map<Long, Integer> sizeToNumFiles = storeReserveMap.getOrDefault(storeId, new HashMap<>());
      sizeToNumFiles.put(size, count);
      storeReserveMap.put(storeId, sizeToNumFiles);
      return this;
    }
  }
}
