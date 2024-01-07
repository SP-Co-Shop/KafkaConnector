import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jeong.config.HdfsConfig;
import org.jeong.hdfs.HdfsClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsWriteTest {


    @Test
    public void hadoop_write_test() throws IOException, InterruptedException{
        Map<String, String> props = new HashMap<>();
        props.put("hdfs.path", "/user/jeong/data/stage");

        HdfsConfig config = new HdfsConfig(props);
        HdfsClient hdfsClient = new HdfsClient(config);
        hdfsClient.init();

        String data = "{ \"traceId\": \"9b165bd0-fdb3-42c8-858f-572d322b7bab\", \"clientIp\": \"0:0:0:0:0:0:0:1\", \"time\": \"2024-01-06T20:55:50.072666\", \"path\": \"/seller/create\", \"method\": \"POST\", \"requestBody\": {    \"email\" : \"MemberA@gmail.com\",    \"password\":\"Test\",    \"name\":\"MemberA\",    \"phoneNumber\":\"010-0000-0000\",    \"address\":\"local\"}, \"responseBody\": null, \"statusCode\": \"201\", \"elapsedTimeMillis\": -40}";  // value
        /**
          *  임시 생성값
          *  실제 Topic X
         */
        String topic = "test-topic";
        Integer partition = 0;

        SinkRecord sinkRecord = new SinkRecord(
                topic,   // topic
                partition,            // partition
                Schema.STRING_SCHEMA, // key schema
                "key1",       // key
                Schema.STRING_SCHEMA,  // value schema
                data,
                123456789L    // timestamp
        );

        List<SinkRecord> list = new ArrayList<>();
        list.add(sinkRecord);
        hdfsClient.write(list, partition);
    }
}
