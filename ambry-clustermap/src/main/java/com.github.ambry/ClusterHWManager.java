package com.github.ambry;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * ClusterHWManager constructs and modifies clusterHW objects. I.e., it manages the relationships among physical
 * components in an Ambry instance. Also provides hooks for SerDe of clusterHW objects.
 *
 * Ensures 'referential integrity' of relationships among physical components. E.g., that all disks belong to a data
 * node that exists, that all data nodes belong to a datacenter that exists, and so on.
 *
 * Ensures uniqueness of each physical component. E.g., disk ids, data node hostnames, and datacenter names.
 */
// TODO: Look into jsonMapper.readValue(string, classOf[HashMap[String, String]]) to SerDe via HashMap rather than via POJO.
public class ClusterHWManager {
    private ClusterHW clusterHW;

    // Structures used to ensure all datacenter names, dataNode hostnames, and disk IDs are unique
    private Set<String> datacenterNames;
    private Set<String> dataNodeNames;
    private long nextDiskId;
    private Set<Long> diskIds;

    public ClusterHWManager() {
        this.clusterHW = new ClusterHW();
        this.datacenterNames = new HashSet<String>();
        this.dataNodeNames =  new HashSet<String>();
        this.nextDiskId = 0;
        this.diskIds =  new HashSet<Long>();
    }

    public ClusterHWManager(ClusterHW clusterHW) {
        this.clusterHW = clusterHW;
        this.datacenterNames = new HashSet<String>();
        this.dataNodeNames =  new HashSet<String>();
        this.diskIds =  new HashSet<Long>();

        for (Datacenter datacenter : clusterHW.getDatacenters()) {
            if (datacenterNames.contains(datacenter.getName())) {
                throw new IllegalArgumentException("Datacenter name is not unique: " + datacenter.getName());
            }
            datacenterNames.add(datacenter.getName());
        }

        for (DataNode dataNode : clusterHW.getDataNodes()) {
            if (dataNodeNames.contains(dataNode.getHostname())) {
                throw new IllegalArgumentException("DataNode name is not unique: " + dataNode.getHostname());
            }
            if (!datacenterNames.contains(dataNode.getDatacenter())) {
                throw new IllegalArgumentException("Datacenter does not exist: " + dataNode.getDatacenter());
            }
            dataNodeNames.add(dataNode.getHostname());
        }

      /*
        long maxDiskId = -1;
        for (Disk disk : clusterHW.getDisks()) {
            if (diskIds.contains(disk.getDiskId())) {
                throw new IllegalArgumentException("Disk id is not unique: " + disk.getDiskId());
            }
            disk.validate();
            if (!dataNodeNames.contains(disk.getDataNode())) {
                throw new IllegalArgumentException("DataNode hostname does not exists: " + disk.getDataNode());
            }
            diskIds.add(disk.getDiskId());
            if(disk.getDiskId() > maxDiskId) {
                maxDiskId = disk.getDiskId();
            }
        }
        if (maxDiskId == -1) {
            this.nextDiskId = 0;
        } else {
            this.nextDiskId = maxDiskId + 1;
        }
        */
    }

    // TODO: Do these (toJson && makeFromJson) belong in ClusterHW rather than ClusterHWManager?
    public void toJson(String filename) {

        ObjectMapper mapper = new ObjectMapper();
        try {
            File clusterFile = new File(filename);

            mapper.writeValue(clusterFile, clusterHW);

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    static public ClusterHWManager makeFromJson(String filename) {
        ObjectMapper mapper = new ObjectMapper();
        File clusterFile = new File(filename);

        ClusterHW clusterHW = null;
        try {
            clusterHW = mapper.readValue(clusterFile, ClusterHW.class);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return new ClusterHWManager(clusterHW);
    }

    public ClusterHW getClusterHW() {
        return clusterHW;
    }

    public boolean isValidDiskId(long diskId) {
        return diskIds.contains(diskId);
    }

    public boolean isValidDataNodeName(String dataNodeName) {
        return dataNodeNames.contains(dataNodeName);
    }

    public boolean isValidDatacenterName(String datacenterName) {
        return datacenterNames.contains(datacenterName);
    }

    public void addNewDatacenter(String datacenterName) {
        if (datacenterNames.contains(datacenterName)) {
            throw new IllegalArgumentException("Datacenter name " + datacenterName + " already exists.");
        }

        Datacenter dc = new Datacenter(null, datacenterName);
        clusterHW.addDatacenter(dc);
        datacenterNames.add(datacenterName);
    }

    public void addNewDataNode(String datacenterName, String nodeHostname) {
        if (!datacenterNames.contains(datacenterName)) {
            throw new IllegalArgumentException("Datacenter name " + datacenterName + " does not exist.");
        }
        if (dataNodeNames.contains(nodeHostname)) {
            throw new IllegalArgumentException("Hostname " + nodeHostname + " already exists.");
        }

        // TODO: fix this
        DataNode dataNode = new DataNode(null, nodeHostname);
        clusterHW.addDataNode(dataNode);
        dataNodeNames.add(nodeHostname);
    }

    public void addNewDisk(String nodeHostname, long capacityGB) {
        if (!dataNodeNames.contains(nodeHostname)) {
            throw new IllegalArgumentException("Hostname " + nodeHostname + " does not exist.");
        }

        long diskId = nextDiskId;
        diskIds.add(diskId);
        nextDiskId++;

        Disk disk = new Disk(null, new DiskId(diskId), capacityGB);
        clusterHW.addDisk(disk);
    }

    public void addNewDisks(int numDisks, String nodeHostname, long capacityGB) {
        if (numDisks < 1) {
            throw new IllegalArgumentException("Too few disks specified to add (numDisks =" + numDisks + ").");
        }

        for(int i=0; i<numDisks; i++) {
            addNewDisk(nodeHostname, capacityGB);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("nextDiskId : ").append(nextDiskId).append(System.lineSeparator());
        sb.append("datacenters : ");
        for(String datacenterName : datacenterNames) {
            sb.append(datacenterName).append(" ");
        }
        sb.append(System.lineSeparator());
        sb.append("dataNodes : ");
        for(String nodeHostname : dataNodeNames) {
            sb.append(nodeHostname).append(" ");
        }
        sb.append(System.lineSeparator());
        sb.append("disks : ");
        for(Long diskId : diskIds) {
            sb.append(diskId).append(" ");
        }
        return sb.toString();
    }

}

