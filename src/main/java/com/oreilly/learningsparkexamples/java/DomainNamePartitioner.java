package com.oreilly.learningsparkexamples.java;

import org.apache.spark.Partitioner;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by liyazhou on 2017/4/7.
 */
public class DomainNamePartitioner extends Partitioner {
    private int numParts;

    public DomainNamePartitioner(int numParts) {
        this.numParts = numParts;
    }

    @Override
    public int numPartitions() {
        return numParts;
    }

    @Override
    public int getPartition(Object key) {
        String domain = null;
        try {
            domain = new URL((String) key).getHost();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        int code = domain.hashCode() % numParts;
        if (code < 0) {
            return code + this.numPartitions();
        } else {
            return code;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DomainNamePartitioner) {
            DomainNamePartitioner pObj = (DomainNamePartitioner) obj;
            return pObj.numPartitions() == this.numPartitions();
        } else {
            return false;
        }
    }
}
