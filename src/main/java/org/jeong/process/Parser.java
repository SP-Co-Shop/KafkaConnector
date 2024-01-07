package org.jeong.process;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jeong.dto.Record;

public interface Parser {

    public Record parse(SinkRecord record) throws JsonProcessingException;
}
