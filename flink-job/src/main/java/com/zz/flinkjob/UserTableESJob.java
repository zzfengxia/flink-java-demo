package com.zz.flinkjob;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ************************************
 * create by Intellij IDEA
 *
 * @author Francis.zz
 * @date 2021-12-31 17:34
 * ************************************
 */
public class UserTableESJob {
    /**
     * 这里从UserTableJob中已经转储的kafka中消费数据
     */
    private static final String SOURCE_KAFKA_USERINFO = "CREATE TABLE user_info_source2 (\n" +
            "  `user_id` STRING,\n" +
            "  `origin_database` STRING,\n" +
            "  `origin_type` STRING,\n" +
            "  `mobile_no` STRING,\n" +
            "  `register_channel` STRING,\n" +
            "  `gender` STRING,\n" +
            "  `nick_name` STRING,\n" +
            "  `register_voucher` STRING,\n" +
            "  `voucher_type` STRING,\n" +
            "  `register_time` TIMESTAMP(0),\n" +
            "  `login_time` TIMESTAMP(0),\n" +
            "  `user_state` STRING,\n" +
            "  `os_type` STRING,\n" +
            "  `mobile_brand` STRING,\n" +
            "  `mobile_type` STRING,\n" +
            "  `os_ver` STRING,\n" +
            "  `login_ip` STRING,\n" +
            // 这里不能有主键
            // "  PRIMARY KEY (`user_id`) NOT ENFORCED,\n" +
            // 声明 create_time 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark
            "  WATERMARK FOR register_time AS register_time - INTERVAL '5' SECOND\n" +
            ") with (\n" +
            "'connector' = 'kafka',\n" +
            "'topic' = 'maxwell_sink',\n" +
            "'properties.group.id' = 'maxwell_sink-test',\n" +
            // 默认值group-offsets，从消费者组中记录的offset开始
            //"'scan.startup.mode' = 'group-offsets',\n" +
            "'properties.bootstrap.servers' = '172.16.80.133:9092',\n" +
            // 从新的topic中消费数据
            "'format' = 'json'\n" +
            ")";
    /**
     * 输出ES
     */
    private static final String SINK_ES_USERINFO = "CREATE TABLE user_info_es_sink (\n" +
            "  `user_id` STRING,\n" +
            "  `origin_database` STRING,\n" +
            "  `origin_type` STRING,\n" +
            "  `mobile_no` STRING,\n" +
            "  `register_channel` STRING,\n" +
            "  `gender` STRING,\n" +
            "  `nick_name` STRING,\n" +
            "  `register_voucher` STRING,\n" +
            "  `voucher_type` STRING,\n" +
            "  `register_time` STRING,\n" +
            "  `login_time` TIMESTAMP(0),\n" +
            "  `user_state` STRING,\n" +
            "  `os_type` STRING,\n" +
            "  `mobile_brand` STRING,\n" +
            "  `mobile_type` STRING,\n" +
            "  `os_ver` STRING,\n" +
            "  `login_ip` STRING,\n" +
            "  PRIMARY KEY (`user_id`) NOT ENFORCED\n" +
            ") with (\n" +
            "'connector' = 'elasticsearch-6',\n" +
            "'hosts' = 'http://172.16.80.142:9200',\n" +
            "'index' = 'tbl-users-info',\n" +
            "'document-type' = '_doc',\n" +
            // 所有主键连接的连接符，默认“_”，key1_key2
            "'document-id.key-delimiter' = '_'\n" +
            ")";
    private static final String USERINFO_TRANSFER = "INSERT INTO user_info_es_sink\n" +
            "SELECT \n" +
            "`user_id`,\n" +
            "`origin_database`,\n" +
            "`origin_type`,\n" +
            " `mobile_no`,\n" +
            " `register_channel`,\n" +
            " `gender`,\n" +
            " `nick_name`,\n" +
            " `register_voucher`,\n" +
            " `voucher_type`,\n" +
            // 添加时区，否则写入ES会相差8小时
            " DATE_FORMAT(`register_time`, 'yyyy-MM-dd HH:mm:ss+0800') as `register_time`,\n" +
            " `login_time`,\n" +
            " `user_state`,\n" +
            " `os_type`,\n" +
            " `mobile_brand`,\n" +
            " `mobile_type`,\n" +
            " `os_ver`,\n" +
            " `login_ip`\n" +
            "FROM user_info_source2";


    private static final String SINK_ES_STATIC = "CREATE TABLE user_static_sink (\n" +
            "  `window_start` TIMESTAMP(3),\n" +
            "  `window_end` TIMESTAMP(3),\n" +
            "  `total` BIGINT,\n" +
            "  PRIMARY KEY (`window_start`, `window_end`) NOT ENFORCED\n" +
            ") with (\n" +
            "'connector' = 'elasticsearch-6',\n" +
            "'hosts' = 'http://172.16.80.142:9200',\n" +
            "'index' = 'user_static_sink',\n" +
            "'document-type' = '_doc',\n" +
            // 所有主键连接的连接符，默认“_”，key1_key2
            "'document-id.key-delimiter' = '_'\n" +
            ")";
    // 1 DAY会自动计算当天0点到当前时间的
    private static final String USERINFO_STAT = "SELECT window_start, window_end, count(user_id) as total FROM TABLE(\n" +
            " CUMULATE(TABLE user_info_source2, DESCRIPTOR(register_time), INTERVAL '10' SECONDS, INTERVAL '1' DAYS)) \n" +
            " GROUP BY window_start, window_end";


    public static void main(String[] args) throws Exception {
        /*StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);*/
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        tEnv.executeSql(SOURCE_KAFKA_USERINFO);
        tEnv.executeSql(SINK_ES_USERINFO);
        tEnv.executeSql(USERINFO_TRANSFER);
        System.out.println("---------------------------");
        // tEnv.executeSql("select * from user_info_source2").print();
        //tEnv.executeSql("desc user_info_source").print();
        //tEnv.executeSql("select * from user_info_source").print();
        //tEnv.executeSql(USERINFO_STAT).print();
        //tEnv.executeSql(SINK_ES_STATIC);
        tEnv.executeSql(USERINFO_STAT).print();

        /*Table resultTable = tEnv.from("user_trans_record");
        DataStream<Row> resultStream = tEnv.toDataStream(resultTable);*/

        // add a printing sink and execute in DataStream API
        //resultStream.print();

        //bsEnv.execute();

    }
}
