package com.Entities;

import java.util.List;
import java.util.Objects;

public class Topic {

    private String topicName;
    private Partition[] partitions;
    private List<String> bootStrapServers;
    private Integer partitionCount;

    public Topic(String topicName, Partition[] partitions, Integer partitionCount) {
        this.topicName = topicName;
        this.partitions = partitions;
        this.partitionCount = partitionCount;
    }

    public String getTopicName() {
        return topicName;
    }



    public Partition[] getPartitions() {
        return partitions;
    }


    public Integer getPartitionCount() {
        return partitionCount;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Topic topic = (Topic) o;
        return Objects.equals(topicName, topic.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName);
    }
}
