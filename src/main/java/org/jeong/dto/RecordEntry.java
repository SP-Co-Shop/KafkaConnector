package org.jeong.dto;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@AllArgsConstructor
@Getter
@Setter
public class Record {

    @JsonProperty("traceId")
    private String traceId;
    @JsonProperty("clientIp")
    private String clientIp;
    @JsonProperty("time")
    private String time;
    @JsonProperty("path")
    private String path;
    @JsonProperty("method")
    private String method;
    @JsonProperty("requestBody")
    private Map<String, String> requestBody;
    @JsonProperty("responseBody")
    private String responseBody;
    @JsonProperty("statusCode")
    private String statusCode;
    @JsonProperty("elapsedTimeMillis")
    private long elapsedTimeMillis;

}
