package org.jeong.entry;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;


/**
 * Consumer에서 가져오는 데이터의 포맷에 맞도록 JsonProperty를 사용 하여 지정
 * HADOOP의 위치를 지정하기 위해 Time이 필요 하므로 getTime만 따로 구현
 * */

public class RecordEntry {

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

    @JsonProperty("request")
    private JsonNode request;

    @JsonProperty("response")
    private JsonNode response;

    @JsonProperty("statusCode")
    private String statusCode;

    @JsonProperty("elapsedTimeMillis")
    private long elapsedTimeMillis;

    public String getTime() {
        return time;
    }
}
