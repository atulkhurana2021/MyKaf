package com;

import com.DAOLayer.Repository;
import com.Resources.ClientService;
import com.Resources.ClusterManagerService;
import com.Resources.TopicService;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) throws Exception{
        // write your code here

        Repository repository = new Repository();
        ClientService clientService = new ClientService(repository);
        TopicService topicService = new TopicService(repository);
        ClusterManagerService clusterManagerService = new ClusterManagerService(repository);
        clusterManagerService.start(Arrays.asList("server1","server2"));
        topicService.registerTopic("topic1",4);
        clientService.registerProducer("producer1");
        clientService.registerConsumer("group1","consumer1");
        clientService.registerConsumer("group1","consumer2");
        clientService.registerConsumer("group2","consumer3");

        clientService.subscribeTopic("topic1","consumer1");
        clientService.subscribeTopic("topic1","consumer2");
        clientService.subscribeTopic("topic1","consumer3");

        System.out.println(clientService.send("topic1","producer1","hello1"));
        System.out.println( clientService.send("topic1","producer1","hello2"));
        System.out.println(clientService.send("topic1","producer1","hello3"));
        System.out.println(clientService.send("topic1","producer1","hello4"));
        System.out.println(clientService.send("topic1","producer1","hello5"));


        System.out.println(clientService.poll("consumer1"));
        System.out.println(clientService.poll("consumer2"));

        System.out.println(clientService.poll("consumer3"));

        clientService.send("topic1","producer1","hello1");
        clientService.send("topic1","producer1","hello2");
        clientService.send("topic1","producer1","hello3");
        clientService.send("topic1","producer1","hello4");

        System.out.println(clientService.poll("consumer1"));
        System.out.println(clientService.poll("consumer2"));

        System.out.println(clientService.poll("consumer3"));

    }
}