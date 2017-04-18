package com.oreilly.learningsparkexamples.java.demo;

import java.io.Serializable;

/**
 * Created by liyazhou on 2017/4/6.
 */
public class LinkInfo implements Serializable {
    private String topic;

    public LinkInfo(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
