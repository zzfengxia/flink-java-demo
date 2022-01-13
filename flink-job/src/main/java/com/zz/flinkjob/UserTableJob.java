package com.zz.flinkjob;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ************************************
 * create by Intellij IDEA
 *
 * @author Francis.zz
 * @date 2021-12-31 17:34
 * ************************************
 */
public class UserTableJob {
    private static final String SOURCE_KAFKA_USERINFO = "CREATE TABLE user_info_source (\n" +
            "  `origin_database` STRING METADATA FROM 'value.database' VIRTUAL,\n" +
            "  `origin_type` STRING METADATA FROM 'value.type' VIRTUAL,\n" +
            "  `sp_user_id` STRING,\n" +
            "  `card_no` STRING,\n" +
            "  `card_code` STRING,\n" +
            "  `source_chnl` STRING,\n" +
            "  `voucher_type` STRING,\n" +
            "  `register_voucher` STRING,\n" +
            "  `device_model` STRING,\n" +
            "  `phone_number` STRING,\n" +
            "  `se_uid` STRING,\n" +
            "  `mobile_brand` STRING,\n" +
            "  `se_chip_manufacturer` STRING,\n" +
            "  `last_opt_time` TIMESTAMP(0),\n" +
            "  `create_time` TIMESTAMP(0),\n" +
            "  `update_time` TIMESTAMP(0),\n" +
            "  PRIMARY KEY (`sp_user_id`) NOT ENFORCED\n" +
            ") with (\n" +
            "'connector' = 'kafka',\n" +
            "'topic' = 'maxwell',\n" +
            "'properties.group.id' = 'maxwell-test',\n" +
            // 默认值group-offsets，从消费者组中记录的offset开始
            //"'scan.startup.mode' = 'group-offsets',\n" +
            "'properties.bootstrap.servers' = '172.16.80.133:9092',\n" +
            "'format' = 'custom-maxwell-json'\n" +
            ")";
    /**
     * 输出kafka sink要定义key.format
     */
    private static final String SINK_KAFKA_USERINFO = "CREATE TABLE user_info_sink (\n" +
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
            "  PRIMARY KEY (`user_id`) NOT ENFORCED\n" +
            ") with (\n" +
            "'connector' = 'upsert-kafka',\n" +
            "'topic' = 'maxwell_sink',\n" +
            "'properties.bootstrap.servers' = '172.16.80.133:9092',\n" +
            "'key.format' = 'json',\n" +
            "'value.format' = 'json'\n" +
            ")";
    private static final String USERINFO_TRANSFER = "INSERT INTO user_info_sink\n" +
            "SELECT \n" +
            "`sp_user_id` as user_id,\n" +
            "`origin_database`,\n" +
            "`origin_type`,\n" +
            " `phone_number` as `mobile_no`,\n" +
            " `source_chnl` as `register_channel`,\n" +
            " '2' as `gender`,\n" +
            " '' as `nick_name`,\n" +
            " '' as `register_voucher`,\n" +
            " '' as `voucher_type`,\n" +
            " `create_time` as `register_time`,\n" +
            " `update_time` as `login_time`,\n" +
            " '1' as `user_state`,\n" +
            " '' as `os_type`,\n" +
            " `mobile_brand`,\n" +
            " `device_model` as `mobile_type`,\n" +
            " '' as `os_ver`,\n" +
            " '' as `login_ip`\n" +
            "FROM user_info_source";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        tEnv.executeSql(SOURCE_KAFKA_USERINFO);
        tEnv.executeSql(SINK_KAFKA_USERINFO);
        // 转储到新的kafka topic中
        tEnv.executeSql(USERINFO_TRANSFER);
        /*tEnv.executeSql("CREATE TABLE `user_trans_record` (\n" +
                "  `id` INT NOT NULL,\n" +
                "  `card_no` STRING,\n" +
                "  `source_chnl` STRING,\n" +
                "  `channel_id` STRING,\n" +
                "  `device_model` STRING,\n" +
                "  `issuer_card_code` STRING,\n" +
                "  `city_code` STRING,\n" +
                "  `trans_type` STRING,\n" +
                "  `trans_seq` STRING,\n" +
                "  `trans_amt` STRING,\n" +
                "  `terminal` STRING,\n" +
                "  `overdraw_amt` STRING,\n" +
                "  `trans_time` STRING,\n" +
                "  `create_time` TIMESTAMP(0),\n" +
                "  PRIMARY KEY (`id`) NOT ENFORCED\n" +
                ") with (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'maxwell',\n" +
                "'properties.bootstrap.servers' = '172.16.80.133:9092',\n" +
                "'format' = 'maxwell-json'\n" +
                ")");*/
        /*Table resultTable = tEnv.from("user_trans_record");
        DataStream<Row> resultStream = tEnv.toDataStream(resultTable);*/

        // add a printing sink and execute in DataStream API
        //resultStream.print();

        //bsEnv.execute();

    }
}
