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
public class OrderTableJob {
    private static final String SOURCE_KAFKA_USERINFO = "CREATE TABLE `order_detail` (\n" +
            "  `origin_database` STRING METADATA FROM 'value.database' VIRTUAL,\n" +
            "  `origin_type` STRING METADATA FROM 'value.type' VIRTUAL,\n" +
            "  `platform_serial_number` STRING ,\n" +
            "  `settle_date_loc` STRING ,\n" +
            "  `trans_recv_date` STRING ,\n" +
            "  `trans_recv_time` STRING ,\n" +
            "  `loc_trans_date` STRING ,\n" +
            "  `loc_trans_time` STRING ,\n" +
            "  `order_no` STRING ,\n" +
            "  `order_status` STRING ,\n" +
            "  `order_type` STRING ,\n" +
            "  `discount_type` INT,\n" +
            "  `discount_amt` DECIMAL ,\n" +
            "  `mchtDiscount_amt` DECIMAL ,\n" +
            "  `pay_card_type` STRING ,\n" +
            "  `order_amount` DECIMAL ,\n" +
            "  `order_real_amount` DECIMAL ,\n" +
            "  `sk_fee` DECIMAL ,\n" +
            "  `sk_real_fee` DECIMAL ,\n" +
            "  `cz_fee` DECIMAL ,\n" +
            "  `cz_real_fee` DECIMAL ,\n" +
            "  `source_chnl` STRING ,\n" +
            "  `physic_no` STRING ,\n" +
            "  `seId` STRING ,\n" +
            "  `ats` STRING ,\n" +
            "  `imei` STRING ,\n" +
            "  `phone_number` STRING ,\n" +
            "  `device_model` STRING ,\n" +
            "  `card_external_code` STRING ,\n" +
            "  `card_code` STRING ,\n" +
            "  `cplc` STRING ,\n" +
            "  `sp_id` STRING ,\n" +
            "  `appletAid` STRING ,\n" +
            "  `se_chip_manufacturer` STRING ,\n" +
            "  `pay_type` STRING ,\n" +
            "  `valid` STRING ,\n" +
            "  `payment_serial_number` STRING ,\n" +
            "  `sk_req_serial` STRING ,\n" +
            "  `sk_ykt_req` STRING ,\n" +
            "  `sk_ykt_cfm` STRING ,\n" +
            "  `SK_TERM_TRANS_DATE` STRING ,\n" +
            "  `SK_TERM_TRANS_TIME` STRING ,\n" +
            "  `sk_enable_time` STRING ,\n" +
            "  `sk_enable_date` STRING ,\n" +
            "  `sk_valid_end_date` STRING ,\n" +
            "  `sk_annual_inspection_date` STRING ,\n" +
            "  `special_info` STRING ,\n" +
            "  `sk_inspection_flag` STRING ,\n" +
            "  `printing_no` STRING ,\n" +
            "  `sk_cost` DECIMAL ,\n" +
            "  `sk_card_fee` DECIMAL ,\n" +
            "  `sk_old_card_id_type` STRING ,\n" +
            "  `sk_old_card_id_no` STRING ,\n" +
            "  `cz_req_serial` STRING ,\n" +
            "  `systemType` STRING ,\n" +
            "  `system_version` STRING ,\n" +
            "  `terminal_no` STRING ,\n" +
            "  `cardNo` STRING ,\n" +
            "  `card_file_0005` STRING ,\n" +
            "  `card_file_0015` STRING ,\n" +
            "  `network_issuersId` STRING ,\n" +
            "  `city_code` STRING ,\n" +
            "  `issuer_app_version` STRING ,\n" +
            "  `card_inner_no` STRING ,\n" +
            "  `sale_date` STRING ,\n" +
            "  `exp_date` STRING ,\n" +
            "  `card_master_type` STRING ,\n" +
            "  `card_sub_type` STRING ,\n" +
            "  `last_recharge_record` STRING ,\n" +
            "  `last_terminal_no` STRING ,\n" +
            "  `last_trans_time` STRING ,\n" +
            "  `offline_counter` STRING ,\n" +
            "  `online_counter` STRING ,\n" +
            "  `mac1` STRING ,\n" +
            "  `card_balance` DECIMAL ,\n" +
            "  `cz_trans_date` STRING ,\n" +
            "  `cz_trans_time` STRING ,\n" +
            "  `ykt_trans_date` STRING ,\n" +
            "  `ykt_trans_time` STRING ,\n" +
            "  `ykt_src_trans_time` STRING ,\n" +
            "  `cz_ykt_req` STRING ,\n" +
            "  `cz_ykt_cfm` STRING ,\n" +
            "  `tac` STRING ,\n" +
            "  `card_file_abn` STRING ,\n" +
            "  `cz_recharge_time` STRING ,\n" +
            "  `cz_min_recharge` STRING ,\n" +
            "  `cz_max_balance` DECIMAL ,\n" +
            "  `random_num` STRING ,\n" +
            "  `card_verify_code` STRING ,\n" +
            "  `YKT_STATE` STRING ,\n" +
            "  `YKT_STATE_DESC` STRING ,\n" +
            "  `TXN_PAY_TYPE` INT ,\n" +
            "  `TRANS_CODE_OUT` STRING ,\n" +
            "  `TRANS_CODE_CHNL` STRING ,\n" +
            "  `TRANS_CODE_CH_NAME` STRING ,\n" +
            "  `MSG_TYPE` STRING ,\n" +
            "  `PAY_PAN` STRING ,\n" +
            "  `PAY_PAN_TYPE` STRING ,\n" +
            "  `INST_ID_PAY` STRING ,\n" +
            "  `MCHNT_ID_PAY` STRING ,\n" +
            "  `MCHNT_CODE_IN` STRING ,\n" +
            "  `MCHNT_NAME_OUT` STRING ,\n" +
            "  `ACQ_INS_ID` STRING ,\n" +
            "  `ACQ_INS_SEQ` STRING ,\n" +
            "  `CARD_DEPOSIT` STRING ,\n" +
            "  `TRADE_APP_VERSION` STRING ,\n" +
            "  `YKT_SRC_SEQ` STRING ,\n" +
            "  `YKT_SRC_TXNDATE` STRING ,\n" +
            "  `YKT_SEQ` STRING ,\n" +
            "  `YKT_BACK_SEQ` STRING ,\n" +
            "  `YKT_BACK_TXNDATE` STRING ,\n" +
            "  `YKT_BACK_FLAG` STRING ,\n" +
            "  `REQ_SEQ` STRING ,\n" +
            "  `REFUND_TOTAL_AMT` DECIMAL ,\n" +
            "  `REFUND_TOTAL_NUM` DECIMAL ,\n" +
            "  `VOUCHER_NO` STRING ,\n" +
            "  `COS_VER` STRING ,\n" +
            "  `CUSTOMER_NO` STRING ,\n" +
            "  `ACCOUNT_NO` STRING ,\n" +
            "  `TERM_ID_IN` STRING ,\n" +
            "  `REQ_KEY_NUM` DECIMAL ,\n" +
            "  `BATCH_NO` STRING ,\n" +
            "  `AUTH_SEQ` STRING ,\n" +
            "  `LI_AUTH_SEQ` STRING ,\n" +
            "  `AUTH_SETT_DATE` STRING ,\n" +
            "  `SETTLE_DATE_CFM` STRING ,\n" +
            "  `ACQ_INS_SEQ_REQ` STRING ,\n" +
            "  `ACQ_INS_SEQ_CFM` STRING ,\n" +
            "  `PLT_SSN_REQ` STRING ,\n" +
            "  `PLT_SSN_CFM` STRING ,\n" +
            "  `PART_FLAG` INT ,\n" +
            "  `YKT_ORDER` STRING ,\n" +
            "  `write_result` STRING ,\n" +
            "  `create_time` STRING ,\n" +
            "  `update_time` STRING ,\n" +
            "  `qk_real_fee` DECIMAL ,\n" +
            "  `qk_fee` DECIMAL ,\n" +
            "  `event_id` STRING ,\n" +
            "  `auth_code` STRING ,\n" +
            "  `version` INT,\n" +
            "  `reserve` STRING ,\n" +
            "  `userid` STRING ,\n" +
            "  `activity_id` STRING ,\n" +
            "  `up_discount_code` STRING \n" +
            ") with (\n" +
            "'connector' = 'kafka',\n" +
            "'topic' = 'maxwell-order_detail',\n" +
            "'properties.group.id' = 'order-detail-group',\n" +
            // 默认值group-offsets，从消费者组中记录的offset开始
            "'scan.startup.mode' = 'group-offsets',\n" +
            //"'scan.startup.mode' = 'earliest-offset',\n" +
            //"'scan.startup.specific-offsets' = 'partition:0,offset:9040',\n" +
            "'properties.bootstrap.servers' = '172.16.80.133:9092',\n" +
            "'format' = 'custom-maxwell-json'\n" +
            ")";
    /**
     * 输出到Mysql sp_order表
     */
    private static final String SINK_KAFKA_USERINFO = "CREATE TABLE `sp_order_sink` (\n" +
            "  `order_no` STRING, \n" +
            "  `order_status` STRING, \n" +
            "  `order_type` STRING, \n" +
            "  `order_amount` DECIMAL, \n" +
            "  `order_real_amount` DECIMAL, \n" +
            "  `sk_fee` DECIMAL, \n" +
            "  `sk_real_fee` DECIMAL, \n" +
            "  `cz_fee` DECIMAL, \n" +
            "  `cz_real_fee` DECIMAL, \n" +
            "  `source_chnl` STRING, \n" +
            "  `is_third_order` INT,\n" +
            "  `card_no` STRING, \n" +
            "  `physic_no` STRING, \n" +
            "  `se_uid` STRING, \n" +
            "  `imei` STRING, \n" +
            "  `phone_number` STRING, \n" +
            "  `device_model` STRING, \n" +
            "  `issuer_id` STRING, \n" +
            "  `card_code` STRING, \n" +
            "  `cplc` STRING, \n" +
            "  `applet_aid` STRING, \n" +
            "  `sp_id` STRING, \n" +
            "  `se_chip_manufacturer` STRING, \n" +
            "  `pay_type` STRING, \n" +
            "  `valid` STRING, \n" +
            "  `payment_serial_number` STRING, \n" +
            "  `mchnt_code_in` STRING, \n" +
            "  `userid` STRING, \n" +
            "  `order_biz_type` INT,\n" +
            "  `contract_code` STRING, \n" +
            "  `pay_card_type` STRING, \n" +
            "  `reserve` STRING, \n" +
            "  `ext_json_data` STRING, \n" +
            "  `version` INT,\n" +
            "  `create_time` STRING, \n" +
            "  `update_time` STRING, \n" +
            "   PRIMARY KEY (`order_no`) NOT ENFORCED" +
            ") with (\n" +
            " 'connector' = 'jdbc',\n" +
            " 'url' = 'jdbc:mysql://172.16.80.120:3306/vfc_sptsm?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai',\n" +
            " 'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
            " 'table-name' = 'sp_order',\n" +
            " 'username' = 'vfc_sptsm',\n" +
            " 'password' = 'vfc888sptsmProd!#'\n" +
            ")";
    private static final String USERINFO_TRANSFER = "INSERT INTO sp_order_sink\n" +
            "SELECT \n" +
            "  `order_no`, \n" +
            "  `order_status`, \n" +
            "  `order_type`, \n" +
            "  `order_amount`, \n" +
            "  `order_real_amount`, \n" +
            "  `sk_fee`, \n" +
            "  `sk_real_fee`, \n" +
            "  `cz_fee`, \n" +
            "  `cz_real_fee`, \n" +
            "  `source_chnl`, \n" +
            "  `TXN_PAY_TYPE` as `is_third_order`,\n" +
            "  `cardNo` as `card_no`, \n" +
            "  `physic_no`, \n" +
            "  `seId` as `se_uid`, \n" +
            "  `imei`, \n" +
            "  `phone_number`, \n" +
            "  `device_model`, \n" +
            "  `card_external_code` as `issuer_id`, \n" +
            "  `card_code`, \n" +
            "  `cplc`, \n" +
            "  `appletAid` as `applet_aid`, \n" +
            "  `sp_id`, \n" +
            "  `se_chip_manufacturer`, \n" +
            "  `pay_type`, \n" +
            "  `valid`, \n" +
            "  `payment_serial_number` , \n" +
            "  `MCHNT_CODE_IN`, \n" +
            "  `userid` , \n" +
            "  `PART_FLAG` as `order_biz_type`,\n" +
            "  `VOUCHER_NO` as `contract_code`, \n" +
            "  `pay_card_type`, \n" +
            "  `reserve` , \n" +
            "  '' as `ext_json_data`, \n" +
            "  `version`,\n" +
            "  `create_time`, \n" +
            "  `update_time`\n" +
            "FROM order_detail\n";

    private static final String PRINT_TABLE = "CREATE TABLE data_print (\n" +
            "  order_no STRING,\n" +
            "  order_status STRING,\n" +
            "  valid STRING, \n" +
            "  create_time STRING, \n" +
            "  update_time STRING \n" +
            ") WITH (\n" +
            "  'connector' = 'print'\n" +
            ")";
    private static final String PRINT_TABLE_INSERT = "INSERT INTO data_print\n" +
            "SELECT \n" +
            "  `order_no`, \n" +
            "  `order_status`, \n" +
            "  `valid`, \n" +
            "  `create_time`, \n" +
            "  `update_time` \n" +
            "FROM order_detail where update_time > '2022-03-04 14:33:00' and order_status = '18'\n";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        tEnv.executeSql(SOURCE_KAFKA_USERINFO);
        tEnv.executeSql(SINK_KAFKA_USERINFO);
        tEnv.executeSql(USERINFO_TRANSFER);
        //tEnv.executeSql(PRINT_TABLE);
        //tEnv.executeSql(PRINT_TABLE_INSERT);
    }
}
