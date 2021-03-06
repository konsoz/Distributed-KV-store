/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.overlay.lookuptable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.TreeMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.id2203.bootstrapping.NodeAssignment;
import se.kth.id2203.networking.NetAddress;
import se.kth.id2203.overlay.PID;

import java.util.*;

/**
 * LookupTable for nodes assigned to partitions.
 *
 * @author Lars Kroll <lkroll@kth.se>
 */
public class LookupTable implements NodeAssignment {

    private static final long serialVersionUID = -8766981433378303267L;
    final static Logger LOG = LoggerFactory.getLogger(LookupTable.class);
    private TreeMultimap<Integer, PID> partitions = TreeMultimap.create();

    public LookupTable(){
    }

    public LookupTable(LookupTable lookupTable){
        this.partitions = TreeMultimap.create(lookupTable.partitions);
    }

    public int lookupPartitionKey(int keyHash){
        Integer partition = partitions.keySet().floor(keyHash);
        if (partition == null) {
            partition = partitions.keySet().last();
        }
        return partition;
    }

    public Collection<PID> lookup(String key) {
        int keyHash = key.hashCode();
        Integer partition = partitions.keySet().floor(keyHash);
        if (partition == null) {
            partition = partitions.keySet().last();
        }
        return partitions.get(partition);
    }

    public TreeMultimap getMap(){
        return partitions;
    }

    public Collection<PID> lookup(int keyHash) {
        Integer partition = partitions.keySet().floor(keyHash);
        if (partition == null) {
            partition = partitions.keySet().last();
        }
        return partitions.get(partition);
    }

    public int reverseLookup(PID node) {
        for (int key : partitions.keySet()) {
            Collection<PID> partition = partitions.get(key);
            if (partition.contains(node))
                return key;
        }
        return -1;
    }

    public PID getPID(NetAddress address){
        for (int key : partitions.keySet()) {
            Collection<PID> partition = partitions.get(key);
            PID pid = PID.getPID(address, (Set) partition);
            if(pid != null)
                return pid;
        }
        return null;
    }

    public int getEdgeKey(){
        return Collections.max(partitions.keySet());
    }

    public Collection<PID> getNodes() {
        return partitions.values();
    }

    public Set<PID> getNodesSet(){
        Set<PID> nodes = new HashSet<>();
        Iterator iterator = partitions.values().iterator();
        while(iterator.hasNext()){
         nodes.add((PID) iterator.next());
        }
        return nodes;
    }

    public void putPartition(int  partition, Set<PID> nodes){
        partitions.removeAll(partition);
        partitions.putAll(partition, nodes);
    }

    public void putNode(int  partition, PID node){
        partitions.put(partition, node);
    }

    public Collection getPartition(int key){
        return partitions.get(key);
    }

    public void removePartition(int partition){
        partitions.removeAll(partition);
    }

    public int freePartition(int replicationDegree){
        for(int key : partitions.keySet()){
            if(partitions.get(key).size() < replicationDegree*2 -1)
                return key;
        }
        return partitions.keySet().descendingIterator().next();
    }

    public int succ(int key){
        Integer ceiling = partitions.keySet().ceiling(key+1);
        if(ceiling == null){
            Integer first = partitions.keySet().ceiling(-1);
            if(first == null){
                return key;
            } else{
                return first;
            }
        }
        else
            return ceiling;
    }

    public long getNewPid(){
        return Collections.max(getNodes()).pid + 1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LookupTable(\n");
        for (Integer key : partitions.keySet()) {
            sb.append(key);
            sb.append(" -> ");
            sb.append(Iterables.toString(partitions.get(key)));
            sb.append("\n");
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Generates the intial lookuptable. A partition is of size replication-degree*2-1
     *
     * @param nodes             Nodes to assign to partitions
     * @param replicationDegree replication-degree
     * @param keySpace          range between each partition
     * @return
     */
    public static LookupTable generate(ImmutableSet<NetAddress> nodes, int replicationDegree, int keySpace) throws PartitionAssignmentException {
        LookupTable lut = new LookupTable();
        int i = 0;
        int partition = 0;
        int partitionMaxSize = replicationDegree * 2 - 1;
        long pid = 0;
        for (NetAddress node : nodes) {
            lut.partitions.put(partition, new PID(node, pid));
            pid++;
            i++;
            if (i == partitionMaxSize) {
                i = 0;
                partition = partition + keySpace;
            }
        }
        if (i < replicationDegree && i != 0)
            throw new PartitionAssignmentException("Could'nt assign nodes to partition, every partition needs to have atleast enough nodes to fulfill replicationDegree");

            return lut;
    }

}
