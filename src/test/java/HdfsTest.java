import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jeong.config.HdfsConfig;
import org.jeong.hdfs.HdfsClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsTest {

    @Test
    public void hadoopWriteTest() throws IOException, InterruptedException {
        // 설정 초기화
        Map<String, String> props = new HashMap<>();
        props.put("hdfs.path", "/user/jeong/data/stage");
        HdfsConfig config = new HdfsConfig(props);

        // HDFS 클라이언트 초기화 및 시작
        HdfsClient hdfsClient = new HdfsClient(config);
        hdfsClient.init();

        // 테스트 데이터 생성
        String data = "{\"traceId\":\"d6b1f666-8538-4d28-9e47-26fd8453babb\",\"clientIp\":\"0:0:0:0:0:0:0:1\",\"time\":\"2024-01-12T03:46:22.317798\",\"path\":\"/seller/create\",\"method\":\"POST\",\"request\":\"{\\\"email\\\":\\\"MemberA@gmail.com\\\",\\\"password\\\":\\\"Test\\\",\\\"name\\\":\\\"MemberA\\\",\\\"phoneNumber\\\":\\\"010-0000-0000\\\",\\\"address\\\":\\\"local\\\"}\",\"response\":\"{\\\"email\\\":\\\"MemberA@gmail.com\\\",\\\"password\\\":\\\"Test\\\",\\\"name\\\":\\\"MemberA\\\",\\\"phoneNumber\\\":\\\"010-0000-0000\\\",\\\"address\\\":\\\"local\\\"}\",\"statusCode\":\"201\",\"elapsedTimeMillis\":-20}";
        String topic = "log-test-kafka";
        Integer partition = 0;

        // SinkRecord 생성
        SinkRecord sinkRecord = new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "key1", Schema.STRING_SCHEMA, data, 123456789L);

        // HDFS에 쓰기
        List<SinkRecord> records = Collections.singletonList(sinkRecord);
        hdfsClient.write(records, topic, partition);

        // 리소스 정리
        hdfsClient.close();

        // 여기에서 HDFS에 데이터가 정상적으로 쓰여졌는지 검증하는 로직을 추가할 수 있습니다.
        // 예: HDFS에서 파일을 읽어 내용을 검증
    }

}
