package com.DAOLayer;

import com.Entities.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Repository {

    private volatile boolean clusterRunning = false;
    private List<String> bootstarpServers = new ArrayList<>();

    private ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Producer> producers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> topicToLastUsedIndex = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Consumer> consumers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, HashSet> consumerSubscription = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, HashSet<Integer>> consumerTopicPartitions = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> consumerTopicPartitionLastUsedIndex = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConsumerGroup> consumerGroups = new ConcurrentHashMap<>();


    public void setClusterRunning(boolean clusterRunning) {
        this.clusterRunning = clusterRunning;
    }

    public boolean isClusterRunning() {
        return clusterRunning;
    }

    public void addServers(List<String> servers) {
        this.bootstarpServers.addAll(servers);
    }

    public boolean isTopicPresent(String name) {
        return topics.get(name) != null;
    }

    public void addTopic(Topic topic) {
        topics.put(topic.getTopicName(), topic);
    }

    public void addProducer(Producer producer) {
        producers.put(producer.getId(), producer);
    }

    public boolean isProducerPresent(String name) {
        return producers.get(name) != null;
    }

    public Partition[] getPartitionArray(String topic) {
        return topics.get(topic).getPartitions();
    }

    public Integer getLastUsedIndex(String topic) {
        Integer index = topicToLastUsedIndex.get(topic);
        if (index == null)
            index = -1;
        return index;
    }

    public void addUsedIndex(String topic, Integer index) {
        topicToLastUsedIndex.put(topic, index);
    }

    public Topic getTopic(String topic) {
        return topics.get(topic);
    }

    public void addConsumer(Consumer consumer) {
        consumers.put(consumer.getId(), consumer);
    }

    public boolean isConsumerPresent(String name) {
        return consumers.get(name) != null;
    }

    public Consumer getConsumer(String name) {
        return consumers.get(name);
    }


    public void addConsumerGroup(ConsumerGroup consumerGroup) {
        consumerGroups.put(consumerGroup.getGroupId(), consumerGroup);
    }

    public ConsumerGroup getConsumerGroup(String groupId) {
        return consumerGroups.get(groupId);
    }

    public HashSet<String> getConsumerSubscription(String consumer) {
        return consumerSubscription.get(consumer);
    }

    public void addSubscription(String clientId, String topic) {
        HashSet<String> subcriptions = consumerSubscription.get(clientId);
        if (subcriptions == null)
            subcriptions = new HashSet<>();
        subcriptions.add(topic);
        consumerSubscription.put(clientId, subcriptions);
    }


    public Integer getConsumerTopicPartitionLastUsedIndex(String consumerID, String topic, Integer partition) {
        Integer index = consumerTopicPartitionLastUsedIndex.get(consumerID + topic + partition);
        if (index == null)
            index = -1;
        return index;
    }

    public void setConsumerTopicPartitionLastUsedIndex(String consumerID, String topic, Integer partition, int index) {
        consumerTopicPartitionLastUsedIndex.put(consumerID + topic + partition, index);
    }

    public HashMap<String, HashSet<String>> getTopicConsumerGrouping() {
        HashMap<String, HashSet<String>> topicConsumerGrouping = new HashMap<>(); //topic:group, set of consumers
        for (ConsumerGroup consumerGroup : consumerGroups.values()) {
            Map<String, HashSet<String>> getTopicConsumers = consumerGroup.getTopicConsumers();
            for (Map.Entry<String, HashSet<String>> e : getTopicConsumers.entrySet()) {
                topicConsumerGrouping.put(e.getKey() + ":" + consumerGroup.getGroupId(), e.getValue());
            }
        }
        return topicConsumerGrouping;
    }

    public HashSet<Integer> getConsumerTopicPartitions(String consumer, String topic) {
        return consumerTopicPartitions.get(consumer + topic);
    }

    public void addConsumerTopicPartitions(String consumer, String topic, Integer partition) {
        HashSet<Integer> partitions = consumerTopicPartitions.get(consumer + topic);
        if (partitions == null)
            partitions = new HashSet<>();
        partitions.add(partition);
        consumerTopicPartitions.put(consumer + topic, partitions);
    }

    public void resetConsumerTopicPartitions() {
        this.consumerTopicPartitions = new ConcurrentHashMap<>();
        this.consumerTopicPartitionLastUsedIndex = new ConcurrentHashMap<>();
    }
}
