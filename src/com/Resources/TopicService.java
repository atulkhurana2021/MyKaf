package com.Resources;

import com.DAOLayer.Repository;
import com.Entities.Partition;
import com.Entities.Topic;
import com.Utils.Utils;

public class TopicService {

    private Repository repository;
    private Utils utils;


    public TopicService(Repository repository) {
        this.repository = repository;
        this.utils = new Utils(repository);
    }

    public void registerTopic(String name, int partitions) throws Exception {
        utils.validateIfClusterRunning();
        if (name == null || name.isEmpty() || repository.isTopicPresent(name) || partitions <= 0)
            throw new Exception("Invalid Input");
        Topic topic = new Topic(name, utils.createPartitions(name, partitions),partitions);
        repository.addTopic(topic);

    }
}
