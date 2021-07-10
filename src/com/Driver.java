package com;

import com.DAL.Repository;

import java.util.Arrays;

public class Driver {

    public static void main(String[] args) throws Exception {
        // write your code here

        Repository repository = new Repository();
        MyKafManager myKafManager = new MyKafManager(repository);

        myKafManager.start(Arrays.asList("server1", "server2"));
        myKafManager.registerTopic("topic1", 4);
        myKafManager.registerProducer("producer1");
        myKafManager.registerConsumer("group1", "consumer1");
        myKafManager.registerConsumer("group1", "consumer2");
        myKafManager.registerConsumer("group2", "consumer3");

        myKafManager.subscribeTopic("topic1", "consumer1");
        myKafManager.subscribeTopic("topic1", "consumer2");
        myKafManager.subscribeTopic("topic1", "consumer3");

        System.out.println(myKafManager.send("topic1", "producer1", "hello1"));
        System.out.println(myKafManager.send("topic1", "producer1", "hello2"));
        System.out.println(myKafManager.send("topic1", "producer1", "hello3"));
        System.out.println(myKafManager.send("topic1", "producer1", "hello4"));
        System.out.println(myKafManager.send("topic1", "producer1", "hello5"));


        System.out.println(myKafManager.poll("consumer1"));
        System.out.println(myKafManager.poll("consumer2"));

        System.out.println(myKafManager.poll("consumer3"));

        myKafManager.send("topic1", "producer1", "hello1");
        myKafManager.send("topic1", "producer1", "hello2");
        myKafManager.send("topic1", "producer1", "hello3");
        myKafManager.send("topic1", "producer1", "hello4");

        System.out.println(myKafManager.poll("consumer1"));
        System.out.println(myKafManager.poll("consumer2"));

        System.out.println(myKafManager.poll("consumer3"));

        myKafManager.stop();

    }
}