package org.jeong.process;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.jeong.dto.RecordEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class JsonParser {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Map<String,String> map = new HashMap<String,String>();

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static RecordEntry parse(SinkRecord record) throws JsonProcessingException, ParseException {



        Logger logger = LoggerFactory.getLogger("JsonParser");

        logger.info("=================Valid======================");
        objectMapper.configure()


        try {
            logger.info("=================Parsing======================");
            Map<String,Object> value = objectMapper.readValue(record.value().toString() ,Map.class);
            logger.info("=================VALUE======================");
            logger.info((String) value.get("level"));
            logger.info("=======================================");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

//
//        LocalDateTime dateTime = LocalDateTime.parse(entry.getTime());
//
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd/HH");
//        String customFormattedString = dateTime.format(formatter);
//
//        entry.setTime(customFormattedString);

        return null;
    }
}
