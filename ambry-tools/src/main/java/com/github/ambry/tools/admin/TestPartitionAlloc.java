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
package com.github.ambry.tools.admin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;


class Disk {
  public long capacity;
  public long freeCapacity;
  public int id;
  public Node node;

  public Disk(int id, long capacity, Node node) {
    this.capacity = capacity;
    this.freeCapacity = capacity;
    this.id = id;
    this.node = node;
  }
}

class Node {
  public List<Disk> disks;
  public int id;

  public Node(int id, int numberOfDisks, long capacity) {
    disks = new ArrayList<Disk>(numberOfDisks);
    for (int i = 0; i < numberOfDisks; i++) {
      disks.add(new Disk(i, capacity, this));
    }
    this.id = id;
  }

  public long getFreeCapacity() {
    long freeCapacity = 0;
    for (Disk disk : disks) {
      freeCapacity += disk.freeCapacity;
    }
    return freeCapacity;
  }

  public Disk getDiskWithMostCapacity(long replicaSize) {
    Disk minDisk = null;
    for (Disk disk : disks) {
      if ((minDisk == null || minDisk.freeCapacity < disk.freeCapacity) && disk.freeCapacity >= replicaSize) {
        minDisk = disk;
      }
    }
    return minDisk;
  }
}

class Datacenter {
  public List<Node> nodes;

  public Datacenter(int numberOfNodes, int numberOfDisks, long capacity) {
    nodes = new ArrayList<Node>(numberOfNodes);
    for (int i = 0; i < numberOfNodes; i++) {
      nodes.add(new Node(i, numberOfDisks, capacity));
    }
  }

  public Node getNodeWithMostFreeCapacity() {
    Node minNode = null;
    for (Node node : nodes) {
      if (minNode == null || minNode.getFreeCapacity() < node.getFreeCapacity()) {
        minNode = node;
      }
    }
    return minNode;
  }
}

class Partition {
  List<Disk> disks;

  public Partition(List<Disk> disks) {
    this.disks = disks;
  }
}

public class TestPartitionAlloc {

  public static void main(String args[]) {

    // 1. strategy 1
    Datacenter dc = new Datacenter(12, 14, 1099511627776L);
    int numberOfPartitions = 100;
    int numberOfReplicas = 3;
    long replicaSize = 107374182400L;
    List<Partition> partitions = new ArrayList<Partition>(numberOfPartitions);

    Strategy1(dc, partitions, numberOfPartitions, numberOfReplicas, replicaSize);
    System.out.println("Strategy 1");
    Output(dc, partitions);
    partitions.clear();

    dc = new Datacenter(12, 14, 1099511627776L);
    Strategy2(dc, partitions, numberOfPartitions, numberOfReplicas, replicaSize);
    System.out.println("Strategy 2");
    Output(dc, partitions);
    partitions.clear();

    dc = new Datacenter(12, 14, 1099511627776L);
    Strategy3(dc, partitions, numberOfPartitions, numberOfReplicas, replicaSize);
    System.out.println("Strategy 3");
    Output(dc, partitions);
    partitions.clear();

    // This is the best strategy with static allocation map. Any two disks differ by atmost 1 replica
    dc = new Datacenter(12, 14, 1099511627776L);
    Strategy4(dc, partitions, numberOfPartitions, numberOfReplicas, replicaSize);
    System.out.println("Strategy 4");
    Output(dc, partitions);
    partitions.clear();
  }

  // shuffle nodes and disks and choose the first for each replica
  public static void Strategy1(Datacenter dc, List<Partition> partitions, int numberOfPartitions, int numberOfReplicas,
      long replicaSize) {
    for (int i = 0; i < numberOfPartitions; i++) {
      List<Node> nodes = dc.nodes;
      Collections.shuffle(nodes);
      List<Disk> allocatedDisks = new ArrayList<Disk>();

      for (Node dataNode : nodes) {
        if (allocatedDisks.size() == numberOfReplicas) {
          break;
        }
        List<Disk> shuffledDisks = dataNode.disks;
        Collections.shuffle(shuffledDisks);

        for (Disk disk : shuffledDisks) {
          if (disk.freeCapacity >= replicaSize) {
            allocatedDisks.add(disk);
            disk.freeCapacity = disk.freeCapacity - replicaSize;
            break; // Only one replica per DataNodeId.
          }
        }
      }
      partitions.add(new Partition(allocatedDisks));
    }
  }

  // choose a random node and disk for each replica
  public static void Strategy2(Datacenter dc, List<Partition> partitions, int numberOfPartitions, int numberOfReplicas,
      long replicaSize) {
    Random rand = new Random(System.currentTimeMillis());
    for (int i = 0; i < numberOfPartitions; i++) {
      List<Node> nodes = dc.nodes;
      List<Disk> allocatedDisks = new ArrayList<Disk>();
      int j = 0;
      while (j < numberOfReplicas) {
        int index = rand.nextInt(nodes.size());
        Set<Integer> checkedDisks = new HashSet<Integer>();
        while (true) {
          List<Disk> shuffledDisks = nodes.get(index).disks;
          int diskIndex = rand.nextInt(shuffledDisks.size());
          Disk disk = shuffledDisks.get(diskIndex);
          if (!checkedDisks.contains(disk.id)) {
            if (disk.freeCapacity >= replicaSize) {
              allocatedDisks.add(disk);
              disk.freeCapacity = disk.freeCapacity - replicaSize;
              j++;
              break;
            }
            checkedDisks.add(disk.id);
          }
        }
      }
      partitions.add(new Partition(allocatedDisks));
    }
  }

  // shuffle nodes and pick the first while disk is chosen based on most capacity left
  public static void Strategy3(Datacenter dc, List<Partition> partitions, int numberOfPartitions, int numberOfReplicas,
      long replicaSize) {
    for (int i = 0; i < numberOfPartitions; i++) {
      List<Node> nodes = dc.nodes;
      Collections.shuffle(nodes);
      List<Disk> allocatedDisks = new ArrayList<Disk>();

      for (Node dataNode : nodes) {
        if (allocatedDisks.size() == numberOfReplicas) {
          break;
        }
        Disk disk = dataNode.getDiskWithMostCapacity(replicaSize);
        allocatedDisks.add(disk);
        disk.freeCapacity = disk.freeCapacity - replicaSize;
      }
      partitions.add(new Partition(allocatedDisks));
    }
  }

  // choose node and disk based on most capacity left
  public static void Strategy4(Datacenter dc, List<Partition> partitions, int numberOfPartitions, int numberOfReplicas,
      long replicaSize) {
    for (int i = 0; i < numberOfPartitions; i++) {
      List<Disk> allocatedDisks = new ArrayList<Disk>();

      for (int j = 0; j < numberOfReplicas; j++) {
        Node node = dc.getNodeWithMostFreeCapacity();
        Disk disk = node.getDiskWithMostCapacity(replicaSize);
        allocatedDisks.add(disk);
        disk.freeCapacity = disk.freeCapacity - replicaSize;
      }
      partitions.add(new Partition(allocatedDisks));
    }
  }

  public static void Output(Datacenter dc, List<Partition> partitions) {
    Map<String, List<Integer>> diskmap = new HashMap<String, List<Integer>>();

    for (int i = 0; i < dc.nodes.size(); i++) {
      for (int j = 0; j < dc.nodes.get(i).disks.size(); j++) {
        diskmap.put(dc.nodes.get(i).disks.get(j).id + ":" + dc.nodes.get(i).id, new ArrayList<Integer>());
      }
    }

    System.out.println("Partitions");
    for (int i = 0; i < partitions.size(); i++) {
      System.out.print("Partition Id " + i);
      for (int j = 0; j < partitions.get(i).disks.size(); j++) {
        List<Integer> replicas =
            diskmap.get(partitions.get(i).disks.get(j).id + ":" + partitions.get(i).disks.get(j).node.id);
        replicas.add(i);
        diskmap.put(partitions.get(i).disks.get(j).id + ":" + partitions.get(i).disks.get(j).node.id, replicas);

        System.out.print(
            " ReplicaId " + partitions.get(i).disks.get(j).id + ":" + partitions.get(i).disks.get(j).node.id + " - "
                + partitions.get(i).disks.get(j).freeCapacity);
      }
      System.out.println();
    }

    System.out.println("Distribution by disks");
    for (Map.Entry<String, List<Integer>> entry : diskmap.entrySet()) {
      System.out.print(" diskId " + entry.getKey());
      for (int i = 0; i < entry.getValue().size(); i++) {
        System.out.print(" partitionId " + entry.getValue().get(i) + " ");
      }
      System.out.println();
    }
  }
}
