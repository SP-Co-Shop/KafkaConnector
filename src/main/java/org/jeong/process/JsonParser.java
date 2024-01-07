package org.jeong.process;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jeong.dto.RecordEntry;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;

public class JsonParser {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static RecordEntry parse(SinkRecord record) throws JsonProcessingException, ParseException {

        String value = Objects.requireNonNull(record.value().toString(),"Record value cannot be null");

        ObjectMapper objectMapper = new ObjectMapper();
        RecordEntry entry = objectMapper.readValue(value, RecordEntry.class);

        LocalDateTime dateTime = LocalDateTime.parse(entry.getTime());

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH");
        String customFormattedString = dateTime.format(formatter);

        entry.setTime(customFormattedString);

        return entry;
    }
}
