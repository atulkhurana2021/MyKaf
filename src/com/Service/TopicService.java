package com.Service;

import com.DAL.Repository;
import com.Entities.Topic;
import com.Utils.GenericHelper;

public class TopicService {

    private Repository repository;
    private GenericHelper genericHelper;


    public TopicService(Repository repository) {
        this.repository = repository;
        this.genericHelper = new GenericHelper(repository);
    }

    public void registerTopic(String name, int partitions) throws Exception {
        genericHelper.validateIfClusterRunning();
        if (name == null || name.isEmpty() || repository.isTopicPresent(name) || partitions <= 0)
            throw new Exception("Invalid nam or no. of partitions are Invalid");
        Topic topic = new Topic(name, genericHelper.createPartitions(name, partitions), partitions);
        repository.addTopic(topic);

    }
}
