package com.Utils;

import com.DAOLayer.Repository;
import com.Entities.Partition;

public class Utils {

    private Repository repository;

    public Utils(Repository repository) {
        this.repository = repository;
    }

    public void validateIfClusterRunning() throws Exception {
        if (!repository.isClusterRunning())
            throw new Exception("Cluster not running currently");
    }

    public Partition[] createPartitions(String name, int partitions) {
        Partition partition[] = new Partition[partitions];
        for (int i = 0; i < partitions; i++) {
            partition[i] = new Partition(name + i);
        }
        return partition;
    }

}
