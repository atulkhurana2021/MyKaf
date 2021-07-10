package com;

import com.DAL.Repository;
import com.Entities.*;
import com.Service.ClusterManagerService;
import com.Service.ConsumerService;
import com.Service.ProducerService;
import com.Service.TopicService;

import java.util.*;

public class MyKafManager {

    private Repository repository;
    private ClusterManagerService clusterManagerService;
    private ConsumerService consumerService;
    private ProducerService producerService;
    private TopicService topicService;

    public MyKafManager(Repository repository) {
        this.repository = repository;
        clusterManagerService = new ClusterManagerService(repository);
        consumerService = new ConsumerService(repository);
        producerService = new ProducerService(repository);
        topicService = new TopicService(repository);
    }

    public void start(List<String> bootstrapServers) throws Exception {
        clusterManagerService.start(bootstrapServers);
    }

    public void stop() throws Exception {
        clusterManagerService.stop();
    }

    public void registerConsumer(String groupId, String clientId) throws Exception {
        consumerService.registerConsumer(groupId, clientId);
    }

    public void subscribeTopic(String topic, String clientId) throws Exception {
        consumerService.subscribeTopic(topic, clientId);
    }

    public List<Record> poll(String consumerId) throws Exception {
        return consumerService.poll(consumerId);
    }

    public void registerProducer(String clientId) throws Exception {
        producerService.registerProducer(clientId);
    }

    public Record send(String topic, String producerId, Object data) throws Exception {
        return producerService.send(topic, producerId, data);
    }

    public void registerTopic(String name, int partitions) throws Exception {
        topicService.registerTopic(name, partitions);
    }


}
