package com.Entities;


import java.util.List;

public class Producer {

    private String id;
    private List<String> bootstrapServers;
    private Object SerializerClass;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Object getSerializerClass() {
        return SerializerClass;
    }

    public void setSerializerClass(Object serializerClass) {
        SerializerClass = serializerClass;
    }
}
