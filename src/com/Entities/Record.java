package com.Entities;

public class Record {

    private Object value;
    private int Offset;
    private String pid;

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getOffset() {
        return Offset;
    }

    public void setOffset(int offset) {
        Offset = offset;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    @Override
    public String toString() {
        return "Record{" +
                "value=" + value +
                ", Offset=" + Offset +
                ", pid='" + pid + '\'' +
                '}';
    }
}
