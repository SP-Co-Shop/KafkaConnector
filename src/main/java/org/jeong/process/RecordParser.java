package org.jeong.process;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jeong.entry.RecordEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

public class RecordParser {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static RecordEntry parse(SinkRecord record) throws JsonProcessingException, ParseException {

        Logger logger = LoggerFactory.getLogger("JsonParser");

        try {
            RecordEntry recordEntry = objectMapper.readValue(record.value().toString(), RecordEntry.class);
            return recordEntry;
        } catch (JsonProcessingException e) {
            logger.error("Error converting String to JSON. Cause: {}", e.getMessage());
            return null;
        }
    }
}