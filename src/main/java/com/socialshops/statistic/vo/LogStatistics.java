package com.socialshops.statistic.vo;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by liyazhou on 2017/3/21.
 */
public class LogStatistics implements Serializable {

    private Timestamp statistics_date;
    private String statistics_mobile_app_id;
    private String statistics_name;
    private Integer statistics_count;

    public Timestamp getStatistics_date() {
        return statistics_date;
    }

    public void setStatistics_date(Timestamp statistics_date) {
        this.statistics_date = statistics_date;
    }

    public String getStatistics_mobile_app_id() {
        return statistics_mobile_app_id;
    }

    public void setStatistics_mobile_app_id(String statistics_mobile_app_id) {
        this.statistics_mobile_app_id = statistics_mobile_app_id;
    }

    public String getStatistics_name() {
        return statistics_name;
    }

    public void setStatistics_name(String statistics_name) {
        this.statistics_name = statistics_name;
    }

    public Integer getStatistics_count() {
        return statistics_count;
    }

    public void setStatistics_count(Integer statistics_count) {
        this.statistics_count = statistics_count;
    }
}
