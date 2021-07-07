package com.Resources;

import com.DAOLayer.Repository;
import com.Entities.*;
import com.Utils.Utils;

import java.util.*;

public class ClientService {

    private Repository repository;
    private Utils utils;

    public ClientService(Repository repository) {
        this.repository = repository;
        this.utils = new Utils(repository);
    }

    public void registerProducer(String clientId) throws Exception {
        utils.validateIfClusterRunning();
        if (clientId == null || clientId.isEmpty() || repository.isProducerPresent(clientId)) {
            throw new Exception("ClientID nme is either null or topic already present");
        }
        Producer producer = new Producer();
        producer.setId(clientId);
        repository.addProducer(producer);
    }

    public void registerConsumer(String groupId, String clientId) throws Exception {
        utils.validateIfClusterRunning();
        if (clientId == null || clientId.isEmpty() || repository.isConsumerPresent(clientId) || groupId == null || groupId.isEmpty()) {
            throw new Exception("Invalid Input");
        }
        Consumer consumer = new Consumer();
        consumer.setId(clientId);
        consumer.setGroupId(groupId);
        consumer.setAutoCommit(true);
        repository.addConsumer(consumer);

    }

    public void subscribeTopic(String topic, String clientId) throws Exception {
        utils.validateIfClusterRunning();
        if (clientId == null || clientId.isEmpty() || !repository.isConsumerPresent(clientId) || !repository.isTopicPresent(topic)) {
            throw new Exception("Invalid Input");
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
        rearrangePartitionsWithGroups();
    }

    public Record send(String topic, String producerId, Object data) throws Exception {
        utils.validateIfClusterRunning();
        if (!repository.isTopicPresent(topic) || !repository.isProducerPresent(producerId)) {
            throw new Exception("Invalid Input");
        }
        int lastUsedIndex = repository.getLastUsedIndex(topic);
        int partitionIndex = 0;
        int totalPartitions = repository.getTopic(topic).getPartitionCount();
        partitionIndex = (lastUsedIndex + 1) % totalPartitions;
        repository.addUsedIndex(topic, partitionIndex);

        Partition partition = repository.getPartitionArray(topic)[partitionIndex];
        return partition.addRecord(data);

    }

    public List<Record> poll(String consumerId) throws Exception {
        utils.validateIfClusterRunning();
        if (!repository.isConsumerPresent(consumerId)) {
            throw new Exception("Invalid Input");
        }
        List<Record> recordList = new ArrayList<>();
        Set<String> topics = repository.getConsumerSubscription(consumerId);
        for (String topic : topics) {
            HashSet<Integer> hashSet = repository.getConsumerTopicPartitions(consumerId, topic);
            Partition[] partitions = repository.getPartitionArray(topic);
            for (Integer partitionIndex : hashSet) {
                while (true){
                    int lastUsedIndex = repository.getConsumerTopicPartitionLastUsedIndex(consumerId, topic, partitionIndex);
                    Record record = partitions[partitionIndex].getRecordFromIndex((lastUsedIndex + 1) % repository.getTopic(topic).getPartitionCount());
                    if (record != null) {
                        repository.setConsumerTopicPartitionLastUsedIndex(consumerId, topic, partitionIndex, lastUsedIndex + 1);
                        recordList.add(record);
                    }
                     else break;
                }

            }
        }


        return recordList;
    }

    private void rearrangePartitionsWithGroups() {
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
