package org.jeong.process;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jeong.dto.Record;

public class JsonParser implements Parser {

    public Record parse(SinkRecord record) throws JsonProcessingException {
        String value = record.value().toString();

        ObjectMapper objectMapper = new ObjectMapper();

        return objectMapper.readValue(value,Record.class);
    }
}
