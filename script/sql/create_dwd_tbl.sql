-- 1. 交易域加购事务事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_trade_cart_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '加购时间',
    `sku_num`     BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_trade_cart_add_inc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
        );

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table bigdata_offline_v1_ws.dwd_trade_cart_add_inc partition(ds = '20250918')
select id,
       user_id,
       sku_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       sku_num
from bigdata_offline_v1_ws.ods_cart_info
where ds = ${bizdate};

--2. 交易域下单事务事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT '商品ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`      STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `date_id`               STRING COMMENT '下单日期ID',
    `create_time`           STRING COMMENT '下单时间',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(38, 18) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(38, 18) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(38, 18) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(38, 18) COMMENT '最终价格分摊'
) COMMENT '交易域下单事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_trade_order_detail_inc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
        );

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table bigdata_offline_v1_ws.dwd_trade_order_detail_inc partition(ds = '20250918')
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       sku_num,
       split_original_amount,
       nvl(split_activity_amount, 0.0),
       nvl(split_coupon_amount, 0.0),
       split_total_amount
from (select id,
             order_id,
             sku_id,
             create_time,
             sku_num,
             sku_num * order_price split_original_amount,
             split_total_amount,
             split_activity_amount,
             split_coupon_amount
      from bigdata_offline_v1_ws.ods_order_detail
      where ds = ${bizdate}) od
         left join
     (select id,
             user_id,
             province_id
      from bigdata_offline_v1_ws.ods_order_info
      where ds = ${bizdate}) oi
     on od.order_id = oi.id
         left join
     (select order_detail_id,
             activity_id,
             activity_rule_id
      from bigdata_offline_v1_ws.ods_order_detail_activity
      where ds = ${bizdate}) act
     on od.id = act.order_detail_id
         left join
     (select order_detail_id,
             coupon_id
      from bigdata_offline_v1_ws.ods_order_detail_coupon
      where ds = ${bizdate}) cou
     on od.id = cou.order_detail_id;

--3. 交易域支付成功事务事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_trade_pay_detail_suc_inc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_trade_pay_detail_suc_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT 'SKU_ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`      STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`               STRING COMMENT '支付日期ID',
    `callback_time`         STRING COMMENT '支付成功时间',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(38, 18) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(38, 18) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(38, 18) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(38, 18) COMMENT '支付金额'
) COMMENT '交易域支付成功事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_trade_pay_detail_suc_inc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
        );

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table bigdata_offline_v1_ws.dwd_trade_pay_detail_suc_inc partition(ds = '20250918')
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time,'yyyy-MM-dd') date_id,
    callback_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
(
    select
        id,
        order_id,
        sku_id,
        sku_num,
        sku_num * order_price split_original_amount,
        split_total_amount,
        split_activity_amount,
        split_coupon_amount
    from bigdata_offline_v1_ws.ods_order_detail
    where ds = ${bizdate}
) od
join
(
    select
        user_id,
        order_id,
        payment_type,
        callback_time
    from bigdata_offline_v1_ws.ods_payment_info
    where ds=${bizdate}
    and payment_status='1602'
) pi
on od.order_id=pi.order_id
left join
(
    select
        id,
        province_id
    from bigdata_offline_v1_ws.ods_order_info
    where ds = ${bizdate}
) oi
on od.order_id = oi.id
left join
(
    select
        order_detail_id,
        activity_id,
        activity_rule_id
    from bigdata_offline_v1_ws.ods_order_detail_activity
    where ds = ${bizdate}
) act
on od.id = act.order_detail_id
left join
(
    select
        order_detail_id,
        coupon_id
    from bigdata_offline_v1_ws.ods_order_detail_coupon
    where ds = ${bizdate}
) cou
on od.id = cou.order_detail_id
left join
(
    select
        dic_code,
        dic_name
    from bigdata_offline_v1_ws.ods_base_dic
    where ds=${bizdate}
    and parent_code='11'
) pay_dic
on pi.payment_type=pay_dic.dic_code;

--4. 交易域购物车周期快照事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_trade_cart_full;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_trade_cart_full
(
    `id`         STRING COMMENT '编号',
    `user_id`   STRING COMMENT '用户ID',
    `sku_id`    STRING COMMENT 'SKU_ID',
    `sku_name`  STRING COMMENT '商品名称',
    `sku_num`   BIGINT COMMENT '现存商品件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd//dwd_trade_cart_full'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
    );

insert overwrite table bigdata_offline_v1_ws.dwd_trade_cart_full partition(ds = '20250918')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from bigdata_offline_v1_ws.ods_cart_info
where ds=${bizdate}
and is_ordered='0';

--5. 交易域交易流程累积快照事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_trade_trade_flow_acc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_trade_trade_flow_acc
(
    `order_id`               STRING COMMENT '订单ID',
    `user_id`                STRING COMMENT '用户ID',
    `province_id`           STRING COMMENT '省份ID',
    `order_date_id`         STRING COMMENT '下单日期ID',
    `order_time`             STRING COMMENT '下单时间',
    `payment_date_id`        STRING COMMENT '支付日期ID',
    `payment_time`           STRING COMMENT '支付时间',
    `finish_date_id`         STRING COMMENT '确认收货日期ID',
    `finish_time`             STRING COMMENT '确认收货时间',
    `order_original_amount` DECIMAL(38, 18) COMMENT '下单原始价格',
    `order_activity_amount` DECIMAL(38, 18) COMMENT '下单活动优惠分摊',
    `order_coupon_amount`   DECIMAL(38, 18) COMMENT '下单优惠券优惠分摊',
    `order_total_amount`    DECIMAL(38, 18) COMMENT '下单最终价格分摊',
    `payment_amount`         DECIMAL(38, 18) COMMENT '支付金额'
) COMMENT '交易域交易流程累积快照事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_trade_trade_flow_acc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
    );

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table bigdata_offline_v1_ws.dwd_trade_trade_flow_acc partition(ds = '20250918')
select
    oi.id,
    user_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd'),
    create_time,
    date_format(callback_time,'yyyy-MM-dd'),
    callback_time,
    finish_time,
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    total_amount,
    nvl(payment_amount,0.0),
    nvl(date_format(finish_time,'yyyy-MM-dd'),'9999-12-31')
from
(
    select
        id,
        user_id,
        province_id,
        create_time,
        original_total_amount,
        activity_reduce_amount,
        coupon_reduce_amount,
        total_amount
    from bigdata_offline_v1_ws.ods_order_info
    where ds=${bizdate}
)oi
left join
(
    select
        order_id,
        callback_time,
        total_amount payment_amount
    from bigdata_offline_v1_ws.ods_payment_info
    where ds=${bizdate}
    and payment_status='1602'
)pi
on oi.id=pi.order_id
left join
(
    select
        order_id,
        create_time finish_time
    from bigdata_offline_v1_ws.ods_order_status_log
    where ds=${bizdate}
    and order_status='1004'
)log
on oi.id=log.order_id;

--6. 工具域优惠券使用 (支付) 事务事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_tool_coupon_used_inc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_tool_coupon_used_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT '用户ID',
    `order_id`     STRING COMMENT '订单ID',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用(支付)时间'
) COMMENT '优惠券使用（支付）事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_tool_coupon_used_inc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
    );

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table bigdata_offline_v1_ws.dwd_tool_coupon_used_inc partition(ds = '20250918')
select
    id,
    coupon_id,
    user_id,
    order_id,
    date_format(used_time,'yyyy-MM-dd') date_id,
    date_format(used_time,'yyyy-MM-dd')
from bigdata_offline_v1_ws.ods_coupon_use
where ds=${bizdate}
and used_time is not null;

--7. 互动域收藏商品事务事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_interaction_favor_add_inc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
    );

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table bigdata_offline_v1_ws.dwd_interaction_favor_add_inc partition(ds = '20250918')
select
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    date_format(create_time,'yyyy-MM-dd')
from bigdata_offline_v1_ws.ods_favor_info
where ds=${bizdate};

--8. 流量域页面浏览事务事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份ID',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备ID',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员ID',
    `version_code`   STRING COMMENT 'APP版本号',
    `page_item`       STRING COMMENT '目标ID',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页ID',
    `page_id`          STRING COMMENT '页面ID ',
    `from_pos_id`     STRING COMMENT '点击坑位ID',
    `from_pos_seq`    STRING COMMENT '点击坑位位置',
    `refer_id`         STRING COMMENT '营销渠道ID',
    `date_id`          STRING COMMENT '日期ID',
    `view_time`       STRING COMMENT '跳入时间',
    `session_id`      STRING COMMENT '所属会话ID',
    `during_time`     BIGINT COMMENT '持续时间毫秒'
) COMMENT '流量域页面浏览事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
    );

set hive.cbo.enable=false;
insert overwrite table bigdata_offline_v1_ws.dwd_traffic_page_view_inc partition (ds='20250918')
select
    -- 从log字段解析common结构体中的ar（省份ID）
    get_json_object(log, '$.common.ar') province_id,
    -- 解析common结构体中的ba（手机品牌）
    get_json_object(log, '$.common.ba') brand,
    -- 解析common结构体中的ch（渠道）
    get_json_object(log, '$.common.ch') channel,
    -- 解析common结构体中的is_new（是否首次启动）
    get_json_object(log, '$.common.is_new') is_new,
    -- 解析common结构体中的md（手机型号）
    get_json_object(log, '$.common.md') model,
    -- 解析common结构体中的mid（设备ID）
    get_json_object(log, '$.common.mid') mid_id,
    -- 解析common结构体中的os（操作系统）
    get_json_object(log, '$.common.os') operate_system,
    -- 解析common结构体中的uid（用户ID）
    get_json_object(log, '$.common.uid') user_id,
    -- 解析common结构体中的vc（APP版本号）
    get_json_object(log, '$.common.vc') version_code,
    -- 解析page结构体中的item（目标ID）
    get_json_object(log, '$.page.item') page_item,
    -- 解析page结构体中的item_type（目标类型）
    get_json_object(log, '$.page.item_type') page_item_type,
    -- 解析page结构体中的last_page_id（上页ID）
    get_json_object(log, '$.page.last_page_id') last_page_id,
    -- 解析page结构体中的page_id（当前页面ID）
    get_json_object(log, '$.page.page_id') page_id,
    -- 解析page结构体中的from_pos_id（点击坑位ID）
    get_json_object(log, '$.page.from_pos_id') from_pos_id,
    -- 解析page结构体中的from_pos_seq（点击坑位位置）
    get_json_object(log, '$.page.from_pos_seq') from_pos_seq,
    -- 解析page结构体中的refer_id（营销渠道ID）
    get_json_object(log, '$.page.refer_id') refer_id,
    -- 解析ts（时间戳）并转换为日期ID（yyyy-MM-dd）
    date_format(from_utc_timestamp(cast(get_json_object(log, '$.ts') as bigint)/1000, 'GMT+8'), 'yyyy-MM-dd') date_id,
    -- 解析ts并转换为完整时间（yyyy-MM-dd HH:mm:ss）
    date_format(from_utc_timestamp(cast(get_json_object(log, '$.ts') as bigint)/1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') view_time,
    -- 解析common结构体中的sid（会话ID）
    get_json_object(log, '$.common.sid') session_id,
    -- 解析page结构体中的during_time（持续时间，毫秒）
    cast(get_json_object(log, '$.page.during_time') as bigint) during_time
from bigdata_offline_v1_ws.ods_z_log
-- 筛选条件：分区为当日，且log字段中的page结构体不为null（确保是页面浏览日志）
where ds=${bizdate}
  and get_json_object(log, '$.page') is not null;
set hive.cbo.enable=true;

--9. 用户域用户注册事务事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_user_register_inc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_user_register_inc
(
    `user_id`          STRING COMMENT '用户ID',
    `date_id`          STRING COMMENT '日期ID',
    `create_time`     STRING COMMENT '注册时间',
    `channel`          STRING COMMENT '应用下载渠道',
    `province_id`     STRING COMMENT '省份ID',
    `version_code`    STRING COMMENT '应用版本',
    `mid_id`           STRING COMMENT '设备ID',
    `brand`            STRING COMMENT '设备品牌',
    `model`            STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_user_register_inc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
    );

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table bigdata_offline_v1_ws.dwd_user_register_inc partition(ds = '20250918')
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
(
    select
        id user_id,  -- 直接使用独立字段id，替代data.id
        create_time  -- 直接使用独立字段create_time，替代data.create_time
    from bigdata_offline_v1_ws.ods_user_info  -- 表名改为ods_user_info
    where ds=${bizdate} -- 无type字段，按分区筛选首日全量注册数据
)ui
left join
(
    select
        get_json_object(log, '$.common.uid') as user_id,
        get_json_object(log, '$.common.ch') as channel,
        get_json_object(log, '$.common.ar') as province_id,
        get_json_object(log, '$.common.vc') as version_code,
        get_json_object(log, '$.common.mid') as mid_id,
        get_json_object(log, '$.common.ba') as brand,
        get_json_object(log, '$.common.md') as model,
        get_json_object(log, '$.common.os') as operate_system
    from bigdata_offline_v1_ws.ods_z_log
    where ds=${bizdate}
      and get_json_object(log, '$.page.page_id')='register'
      and get_json_object(log, '$.common.uid') is not null
)log
on ui.user_id = log.user_id;


--10. 用户域用户登录事务事实表
DROP TABLE IF EXISTS bigdata_offline_v1_ws.dwd_user_login_inc;
CREATE EXTERNAL TABLE bigdata_offline_v1_ws.dwd_user_login_inc
(
    `user_id`         STRING COMMENT '用户ID',
    `date_id`         STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`         STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`          STRING COMMENT '设备ID',
    `brand`           STRING COMMENT '设备品牌',
    `model`           STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/bigdata_offline_v1_ws/dwd/dwd_user_login_inc'
    TBLPROPERTIES (
        'orc.compress' = 'snappy',
        'external.table.purge' = 'true'
    );

insert overwrite table bigdata_offline_v1_ws.dwd_user_login_inc partition (ds = '20250918')
select
    get_json_object(log, '$.common.uid') as user_id,
    -- 解析ts转换为日期ID
    date_format(from_utc_timestamp(cast(get_json_object(log, '$.ts') as bigint)/1000, 'GMT+8'), 'yyyy-MM-dd') date_id,
    -- 解析ts转换为登录时间
    date_format(from_utc_timestamp(cast(get_json_object(log, '$.ts') as bigint)/1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') login_time,
    -- 解析log字段中的渠道、设备等信息
    get_json_object(log, '$.common.ch') channel,
    get_json_object(log, '$.common.ar') province_id,
    get_json_object(log, '$.common.vc') version_code,
    get_json_object(log, '$.common.mid') mid_id,
    get_json_object(log, '$.common.ba') brand,
    get_json_object(log, '$.common.md') model,
    get_json_object(log, '$.common.os') operate_system
from (
         select
             log,
             -- 按会话ID分区，取每个会话首条登录记录
             row_number() over (partition by get_json_object(log, '$.common.sid') order by cast(get_json_object(log, '$.ts') as bigint)) rn
         from bigdata_offline_v1_ws.ods_z_log
         where ds=${bizdate}
           and get_json_object(log, '$.page') is not null
           and get_json_object(log, '$.common.uid') is not null
     ) t
where t.rn = 1;