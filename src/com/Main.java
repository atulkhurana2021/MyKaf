package com;

import com.DAOLayer.Repository;
import com.Resources.*;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) throws Exception{
        // write your code here

        Repository repository = new Repository();
        
        
        ProducerService producerService = new ProducerService(repository);
        ConsumerService consumerService = new ConsumerService(repository);
        TopicService topicService = new TopicService(repository);
        ClusterManagerService clusterManagerService = new ClusterManagerService(repository);
        
        
        clusterManagerService.start(Arrays.asList("server1","server2"));
        
        topicService.registerTopic("topic1",4);
        
        producerService.registerProducer("producer1");
        consumerService.registerConsumer("group1","consumer1");
        consumerService.registerConsumer("group1","consumer2");
        consumerService.registerConsumer("group2","consumer3");

        consumerService.subscribeTopic("topic1","consumer1");
        consumerService.subscribeTopic("topic1","consumer2");
        consumerService.subscribeTopic("topic1","consumer3");

        System.out.println(producerService.send("topic1","producer1","hello1"));
        System.out.println( producerService.send("topic1","producer1","hello2"));
        System.out.println(producerService.send("topic1","producer1","hello3"));
        System.out.println(producerService.send("topic1","producer1","hello4"));
        System.out.println(producerService.send("topic1","producer1","hello5"));


        System.out.println(consumerService.poll("consumer1"));
        System.out.println(consumerService.poll("consumer2"));
        System.out.println(consumerService.poll("consumer3"));

        producerService.send("topic1","producer1","hello1");
        producerService.send("topic1","producer1","hello2");
        producerService.send("topic1","producer1","hello3");
        producerService.send("topic1","producer1","hello4");

        System.out.println(consumerService.poll("consumer1"));
        System.out.println(consumerService.poll("consumer2"));
        System.out.println(consumerService.poll("consumer3"));
        
        clusterManagerService.stop();

    }
}
