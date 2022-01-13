package com.zz.flinkjob;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * ************************************
 * create by Intellij IDEA
 *
 * @author Francis.zz
 * @date 2021-12-30 15:58
 * ************************************
 */
public class UserHandleJob {
    public static void main(String[] args) {
        Properties properties = new Properties();
        /*try {
            properties.load(UserHandleJob.class.getClassLoader().getResourceAsStream("application-setting-prod.properties"));
        } catch (IOException e) {
            LogUtil.error("load properties failed, " + e.getMessage());
        }*/
        properties.setProperty("bootstrap.servers", "172.16.80.133:9092");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 指定Topic
        /*Map<KafkaTopicPartition, Long> offsets = new HashedMap();
        offsets.put(new KafkaTopicPartition("topic_name", 0), 11L);
        offsets.put(new KafkaTopicPartition("topic_name", 1), 22L);
        offsets.put(new KafkaTopicPartition("topic_name", 2), 33L);
        consumer.setStartFromSpecificOffsets(offsets);*/

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("maxwell-vfc_sptsm-user_info", new SimpleStringSchema(), properties);
        // 尽可能从最早的记录开始
        myConsumer.setStartFromEarliest();
        DataStream<String> stream = env
                .addSource(myConsumer)
                .name("maxwell-test");

        stream.print();
        try {
            env.execute("kafka 消费任务开始");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
