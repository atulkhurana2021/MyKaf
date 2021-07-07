package com.Entities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ConsumerGroup {
    private String groupId;
    private Map<String, HashSet<String>> topicConsumers= new HashMap<>();

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Map<String, HashSet<String>> getTopicConsumers() {
        return topicConsumers;
    }

    public void setTopicConsumers(Map<String, HashSet<String>> topicConsumers) {
        this.topicConsumers = topicConsumers;
    }
}
