package com.Service;

import com.DAL.Repository;
import com.Entities.Consumer;
import com.Entities.ConsumerGroup;
import com.Entities.Partition;
import com.Entities.Record;
import com.Utils.GenericHelper;

import java.util.*;

public class ConsumerService {

    private Repository repository;
    private GenericHelper genericHelper;

    public ConsumerService(Repository repository) {
        this.repository = repository;
        this.genericHelper = new GenericHelper(repository);
    }

    public void registerConsumer(String groupId, String clientId) throws Exception {
        genericHelper.validateIfClusterRunning();
        if (clientId == null || clientId.isEmpty() || repository.isConsumerPresent(clientId) || groupId == null || groupId.isEmpty()) {
            throw new Exception("Invalid clientId or groupID");
        }
        Consumer consumer = new Consumer();
        consumer.setId(clientId);
        consumer.setGroupId(groupId);
        consumer.setAutoCommit(true);
        repository.addConsumer(consumer);

    }

    public void subscribeTopic(String topic, String clientId) throws Exception {
        genericHelper.validateIfClusterRunning();
        if (clientId == null || clientId.isEmpty() || !repository.isConsumerPresent(clientId) || !repository.isTopicPresent(topic)) {
            throw new Exception("Invalid Topic or clientId");
        }
        repository.addSubscription(clientId, topic);
        String groupId = repository.getConsumer(clientId).getGroupId();
        ConsumerGroup consumerGroup = repository.getConsumerGroup(groupId);
        if (consumerGroup == null) {
            consumerGroup = new ConsumerGroup();
        }
        Map<String, HashSet<String>> topicConsumers = consumerGroup.getTopicConsumers();

        if (topicConsumers == null)
            topicConsumers = new HashMap<>();
        HashSet<String> consumers = topicConsumers.get(topic);
        if (consumers == null) {
            consumers = new HashSet<>();
        }
        consumers.add(clientId);
        topicConsumers.put(topic, consumers);
        consumerGroup.setGroupId(groupId);
        consumerGroup.setTopicConsumers(topicConsumers);
        repository.addConsumerGroup(consumerGroup);
        genericHelper.rearrangePartitionsWithGroups();
    }

    public List<Record> poll(String consumerId) throws Exception {
        genericHelper.validateIfClusterRunning();
        if (!repository.isConsumerPresent(consumerId)) {
            throw new Exception("Invalid consumerId");
        }
        List<Record> recordList = new ArrayList<>();
        Set<String> topics = repository.getConsumerSubscription(consumerId);
        for (String topic : topics) {
            HashSet<Integer> hashSet = repository.getConsumerTopicPartitions(consumerId, topic);
            Partition[] partitions = repository.getPartitionArray(topic);
            for (Integer partitionIndex : hashSet) {
                while (true) {
                    int lastUsedIndex = repository.getConsumerTopicPartitionLastUsedIndex(consumerId, topic, partitionIndex);
                    Record record = partitions[partitionIndex].getRecordFromIndex((lastUsedIndex + 1) % repository.getTopic(topic).getPartitionCount());
                    if (record != null) {
                        if (repository.getConsumer(consumerId).isAutoCommit()) {
                            repository.setConsumerTopicPartitionLastUsedIndex(consumerId, topic, partitionIndex, lastUsedIndex + 1);
                        }
                        recordList.add(record);
                    } else break;
                }

            }
        }


        return recordList;
    }
}
