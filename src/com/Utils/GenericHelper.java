package com.Utils;

import com.DAOLayer.Repository;
import com.Entities.Partition;
import com.Entities.Topic;

import java.util.*;

public class GenericHelper {

    private Repository repository;

    public GenericHelper(Repository repository) {
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

    public void rearrangePartitionsWithGroups() {
        repository.resetConsumerTopicPartitions();
        HashMap<String, HashSet<String>> topicConsumers = repository.getTopicConsumerGrouping();  //topic:group, set of consumers
        for (Map.Entry<String, HashSet<String>> e : topicConsumers.entrySet()) {
            Topic topic = repository.getTopic(e.getKey().split(":")[0]);
            int partitionCount = topic.getPartitionCount();
            String topicName = topic.getTopicName();
            Queue<String> consumerQueue = new LinkedList<>(e.getValue());
            for (int i = 0; i < partitionCount; i++) {
                String consumer = consumerQueue.poll();
                repository.addConsumerTopicPartitions(consumer, topicName, i);
                consumerQueue.offer(consumer);
            }
        }

    }

}
