package com.zhuravishkin.kafkastreamstrigger.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class User {
    @JsonProperty("profile_id")
    private Integer profileId;

    @JsonProperty("bucket_id")
    private Integer bucketId;

    @JsonProperty("first_name")
    private String firstName;

    @JsonProperty("sur_name")
    private String surName;

    @JsonProperty("event_time")
    private Long eventTime;
}
