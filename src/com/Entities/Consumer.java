package com.Entities;


public class Consumer {
    private String groupId;
    private String id;
    private boolean autoCommit ;
    private Object DeSerializerClass;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public Object getDeSerializerClass() {
        return DeSerializerClass;
    }

    public void setDeSerializerClass(Object deSerializerClass) {
        DeSerializerClass = deSerializerClass;
    }
}
