#!/usr/bin/python2.7

import json
import argparse
import sys
from collections import defaultdict


class Node(object):

    def __init__(self, node, datacenter):
        self.node = node
        self.datacenter = datacenter
        self.partitions = set()

    @property
    def hostname(self):
        return self.node["hostname"]

    @property
    def port(self):
        return self.node["port"]

    @property
    def rack_id(self):
        if "rackId" in self.node:
            return self.node["rackId"]
        return -1

    @property
    def datacenter_name(self):
        return self.datacenter["name"]

    def add_partition(self, partition):
        self.partitions.add(partition)

    def __repr__(self):
        return "[hostname: {}, port: {}, dc: {}]".format(
            self.hostname, self.port, self.datacenter_name)

    __str__ = __repr__


class Partition(object):

    def __init__(self, partition):
        self.partition = partition
        self.nodes_by_datacenter = defaultdict(set)

    @property
    def id(self):
        return self.partition["id"]

    def add_node(self, node):
        self.nodes_by_datacenter[node.datacenter_name].add(node)
        node.add_partition(self)

    def racks_used(self, datacenter_name):
        return {node.rack_id for node in self.nodes_by_datacenter[datacenter_name]}

    def __repr__(self):
        return "[id: {}]".format(self.id)

    __str__ = __repr__


class Layout(object):

    def __init__(self, hardware_layout_filename, partition_layout_filename):
        with open(hardware_layout_filename) as f:
            self.hardware_layout = json.load(f)
        with open(partition_layout_filename) as f:
            self.partition_layout = json.load(f)
        self.setup()

    def setup(self):
        self.node_map = {}
        self.partition_map = {}
        self.dc_node_combo_map = defaultdict(lambda: defaultdict(set))
        for datacenter_struct in self.hardware_layout["datacenters"]:
            for node_struct in datacenter_struct["dataNodes"]:
                k = (node_struct["hostname"], node_struct["port"])
                self.node_map[k] = Node(node_struct, datacenter_struct)
        for partition_struct in self.partition_layout["partitions"]:
            partition = Partition(partition_struct)
            for replica_struct in partition_struct["replicas"]:
                k = (replica_struct["hostname"], replica_struct["port"])
                node = self.node_map[k]
                partition.add_node(node)
            for dc, nodes in partition.nodes_by_datacenter.items():
                self.dc_node_combo_map[dc][frozenset(nodes)].add(partition)
            self.partition_map[partition_struct["id"]] = partition

    def rack_id(self, node_host, node_port):
        k = (node_host, node_port)
        if k in self.node_map:
            return self.node_map[k].rack_id
        raise Exception("Node not found")

    def racks_used(self, partition_id, datacenter_name):
        return self.partition_map[partition_id].racks_used(datacenter_name)

    def shared_partitions(self, *nodes):
        return set.intersection(
            *(self.node_map[node].partitions for node in nodes)
        )

    def print_report(self):
        for dc, node_combo_map in self.dc_node_combo_map.items():
            print("In datacenter: {}".format(dc))
            max_combo = max(node_combo_map,
                            key=lambda k: len(node_combo_map[k]))
            print("Num node combos used: {}".format(len(node_combo_map)))
            print("Average partitions sharing a node combo: {}".format(
                sum(len(partitions) for partitions in node_combo_map.values()) / float(len(node_combo_map))))
            print("Max partitions sharing a node combo: {} on the following nodes:".format(
                len(node_combo_map[max_combo])))
            for node in max_combo:
                print(node)

            sum_racks, n_partitions, min_racks = 0, 0, sys.maxsize
            for partition in self.partition_map.values():
                num_racks = len(partition.racks_used(dc))
                if num_racks > 0:
                    n_partitions += 1
                    sum_racks += num_racks
                    if num_racks < min_racks:
                        min_racks = num_racks
            print("Min racks used: {}".format(min_racks))
            print("Average racks used: {}".format(
                float(sum_racks) / n_partitions))
            partitions_per_node = [len(node.partitions) for node in self.node_map.values()
                                   if node.datacenter_name == dc]

            print("")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze node distribution in a partition layout')
    parser.add_argument('hardware_layout',
                        help='the path to the hardware layout file')
    parser.add_argument('partition_layout',
                        help='the path to the partition layout file')

    args = parser.parse_args()
    layout = Layout(args.hardware_layout, args.partition_layout)
    layout.print_report()

if __name__ == "__main__":
    main()
