package com.zz.flinkjob;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * ************************************
 * create by Intellij IDEA
 *
 * @author Francis.zz
 * @date 2022-01-12 17:02
 * ************************************
 */
public class TableMysqlJob {
    private static final String CREATE_CARD_CODE_DICT_TBL = "CREATE TABLE card_code_dict_source (\n" +
            "  `id` INT ,\n" +
            "  `card_external_code` STRING ,\n" +
            "  `process_code` STRING,\n" +
            "  `card_code` STRING,\n" +
            "  `firm_code` STRING,\n" +
            "  `source_chnl` STRING,\n" +
            "  `card_desc` STRING,\n" +
            "  `card_name` STRING,\n" +
            "  `server_url` STRING,\n" +
            "  PRIMARY KEY (`id`) NOT ENFORCED\n" +
            ") with (\n" +
            " 'connector' = 'jdbc',\n" +
            " 'url' = 'jdbc:mysql://localhost:3306/vfc_sptsm?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai',\n" +
            " 'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
            " 'table-name' = 'card_code_dict',\n" +
            " 'username' = 'root',\n" +
            " 'password' = '123456'\n" +
            ")";

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

    public static void main(String[] args) {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        tEnv.executeSql(CREATE_CARD_CODE_DICT_TBL);
        tEnv.executeSql(SOURCE_KAFKA_USERINFO);
        tEnv.executeSql("select * from card_code_dict_source").print();

        // 关联查询，转义字段
        tEnv.executeSql("select b.user_id, a.card_desc from card_code_dict_source a left join user_info_source2 b on a.card_external_code = b.user_id").print();
    }
}
