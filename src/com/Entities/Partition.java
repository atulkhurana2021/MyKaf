package com.Entities;


import java.util.HashMap;
import java.util.Map;

public class Partition {

    private String pid;
    private volatile Integer size;
    private Map<Integer, Record> recordMap;


    public Partition(String pid) {
        this.recordMap = new HashMap<>();
        this.size = 0;
        this.pid = pid;
    }


    public Record addRecord(Object value) {
        Record record = new Record();
        record.setOffset(this.size);
        record.setPid(pid);
        record.setValue(value);
        recordMap.put(this.size++, record);
        return record;
    }

    public Record getRecordFromIndex(int index) {
        return recordMap.get(index);
    }
}
