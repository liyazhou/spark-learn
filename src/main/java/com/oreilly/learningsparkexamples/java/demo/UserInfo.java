package com.oreilly.learningsparkexamples.java.demo;

import java.io.Serializable;
import java.util.List;

/**
 * Created by liyazhou on 2017/4/6.
 */
public class UserInfo implements Serializable {
    private List<String> topics;

    public UserInfo(List<String> topics) {
        this.topics = topics;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }
}
