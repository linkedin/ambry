package com.github.ambry;

import java.util.ArrayList;

/**
 * ClusterLV is the lowest level representation of the physical components (datacenter, data nodes, disks) that
 * comprise an Ambry instance.
 */
public class ClusterHW {
    // TODO: Add version numbe here.
    private ArrayList<Datacenter> datacenters;
    private ArrayList<DataNode> dataNodes;
    private ArrayList<Disk> disks;

    // "simple" constructor needed for JSON SerDe.
    public ClusterHW() {
        this.datacenters = new ArrayList<Datacenter>();
        this.dataNodes = new ArrayList<DataNode>();
        this.disks = new ArrayList<Disk>();
    }

    public ArrayList<Datacenter> getDatacenters() {
        return datacenters;
    }

    public ArrayList<DataNode> getDataNodes() {
        return dataNodes;
    }

    public ArrayList<Disk> getDisks() {
        return disks;
    }

    protected void setDatacenters(ArrayList<Datacenter> datacenters) {
        this.datacenters = datacenters;
    }

    protected void setDataNodes(ArrayList<DataNode> dataNodes) {
        this.dataNodes = dataNodes;
    }

    protected void setDisks(ArrayList<Disk> disks) {
        this.disks = disks;
    }

    public void addDatacenter(Datacenter datacenter) {
        this.datacenters.add(datacenter);
    }

    public void addDataNode(DataNode dataNode) {
        this.dataNodes.add(dataNode);
    }

    public void addDisk(Disk disk) {
        this.disks.add(disk);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("datacenters : ");
        for(Datacenter datacenter : datacenters) {
            sb.append(datacenter.getName()).append(" ");
        }
        sb.append(System.lineSeparator());
        sb.append("dataNodes : ");
        for(DataNode dataNode : dataNodes) {
            sb.append(dataNode.getHostname()).append(" ");
        }
        sb.append(System.lineSeparator());
        sb.append("disks : ");
        for(Disk disk: disks) {
            sb.append(disk.getDiskId()).append(" ");
        }
        return sb.toString();
    }
}
