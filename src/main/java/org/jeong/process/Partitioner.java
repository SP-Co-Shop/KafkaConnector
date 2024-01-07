package org.jeong.process;

import org.apache.kafka.connect.sink.SinkRecord;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Partitioner {
    /**
     * Kafka Record에서 timestamp Filed를 Parsing
     * @param record
     * @return Date
    * */

    public static String getPartitionPath(SinkRecord record){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd/HH");
        return sdf.format(new Date());
    }
}
