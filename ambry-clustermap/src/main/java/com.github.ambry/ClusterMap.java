package com.github.ambry;

/**
 * TODOs for ambry in general
 *  - Standardize on intellij automagic class header
 *  - javadoc conventions? or documentation standard for non-API components
 *  - is there automagic code formatting in intellij that forces a coding convention? I don't care on exact convention.
 *    I do care that all code follows same conventions.
 *  - Are there any exceptions "best practices" we need to agree upon before we get too far?
 *
 * TODOs for ambry-clustermap
 * - Names of all base types in this package need to be reviewed carefully now. Changing names later is a pain,
 *   and names matter!
 * - Pattern of clusterXX, clusterXXManager, and clusterXXTopology is a bit ugly. The lowest level is clusterXX and
 *   is useful for SerDe. ClusterXXManager ensures "referential integrity" of clusterXX and helper methods to update
 *   clusterXX. ClusterXXTopology ensures higher level constraints are maintained and offer helper methods to query
 *   the topology. ClusterMap wraps up the clusterXXTopology classes and offers narrow interfaces for querying.
 * - Should we avoid using bare base types for key types. For example, long for disk ID, long for volume id,
 *   String for hostname, and String for datacenterName? If we use bare base types, some interfaces are not really
 *   typesafe.
 * - toString methods to pretty print all core types (DataNode,Disk,etc.)
 * - Add logging
 * - Need unit tests for each class!
 * - Need javadoc
 */

/**
 * ClusterMap allows components in Ambry to query the topology. This covers both physical and logical aspects of
 * the topology.
 */
public class ClusterMap {
    protected ClusterHWTopology clusterHWTopology;
    protected ClusterLVTopology clusterLVTopology;


    public ClusterMap(ClusterHWTopology clusterHWTopology, ClusterLVTopology clusterLVTopology) {
        this.clusterHWTopology = clusterHWTopology;
        this.clusterLVTopology = clusterLVTopology;
    }

}
