package com.tsg.lq.common;

/**
 * Created by tsguang on 2019/8/22.
 */
public class Message {

    private String topic;

    private String uuid;

    private long idx = -1;

    private String text;

    private int sendCount;

    private long createTime;


    public Message(String topic) {
        this.topic = topic;
        this.uuid = null;
        this.idx = 0L;
        this.text = null;
        this.sendCount = 1;
//        this.createTime = System.currentTimeMillis();
    }

    public Message(String topic, String uuid, Long idx, String text, Integer sendCount) {
        this.topic = topic;
        this.uuid = uuid;
        this.idx = idx;
        this.text = text;
        this.sendCount = sendCount;
    }

    public void reset(){
        this.uuid = null;
        this.idx = -1L;
        this.text = null;
        this.sendCount = 1;
//        this.createTime = System.currentTimeMillis();
    }

    public String getTopic() {
        return topic;
    }

//    public void setTopic(String topic) {
//        this.topic = topic;
//    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public long getIdx() {
        return idx;
    }

    public void setIdx(long idx) {
        this.idx = idx;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getSendCount() {
        return sendCount;
    }

    public void setSendCount(int sendCount) {
        this.sendCount = sendCount;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
}
