

-- 查询元仓m_table
-- select raw_meta from meta.m_table where name = 'wireless_wdm.dwd_user_track_hour' and ds = '20190125';

！！！打印filed 指定个数 100

--获取90-180表的步骤

--1. 生成 json_job_sql_table

insert overwrite table json_job_sql_table partition(ds)  select project_name,regexp_extract(source_xml,'(?s).*<Query>(.*select.*|.*SELECT.*|.*insert.*|.*INSERT.*);</Query>.*',1) as sql, ds from meta.m_instance where ds between '20181201' and '20181231' and regexp_count(source_xml,'get_json_object|GET_JSON_OBJECT')>0 and status = '5';
    --排除为空的情况
insert overwrite table json_job_sql_table partition(ds) select * from json_job_sql_table where sql!='' and ds between '20181201' and '20191231';

--2. 张一鹏生成过滤好的meta_table（spark）

--3. 胡振宇生成json_job_plan_table （spark）

--4. 胡振宇生成json_access_frequency （spark）






--胡振宇的表
meta_table: 过滤后的table meta
table_raw_meta： 用于存放所有raw_meta数据
json_job_sql_table: 用于存放所有带get_json_object的project_name 以及提取出的sql语句
json_job_plan_table:用于存放所有带get_json_object的sql的plan
insert into table json_job_sql_table partition(ds = '20190125')  select project_name,regexp_extract(source_xml,'(?s).*<Query>(.*select.*|.*SELECT.*|.*insert.*|.*INSERT.*);</Query>.*',1) as sql from meta.m_instance where ds = '20190125' and (regexp_count(source_xml,'get_json_object') + regexp_count(source_xml,'GET_JSON_OBJECT'))>0 and status = '5' and sql !='';
insert overwrite table json_job_sql_table partition(ds)  select project_name,regexp_extract(source_xml,'(?s).*<Query>(.*select.*|.*SELECT.*|.*insert.*|.*INSERT.*);</Query>.*',1) as sql, ds from meta.m_instance where ds between '20190301' and '20190324' and regexp_count(source_xml,'get_json_object|GET_JSON_OBJECT')>0 and status = '5';
insert overwrite table table_raw_meta partition(ds) select project_name, name, type, raw_meta,ds from meta.m_table where (type='TABLE' or type='VIEW')  and ds between '20190128' and '20190131';
insert overwrite table json_job_sql_table partition(ds)
 select a.project_name, a.sql ,a.ds
 from (select project_name, regexp_extract(source_xml,'(?s).*<Query>(.*select.*|.*SELECT.*|.*insert.*|.*INSERT.*);</Query>.*',1) as sql, ds
    from meta.m_instance where ds ='20190125'
    and (instr(source_xml,'get_json_object') >0 or instr(source_xml, 'GET_JSON_OBJECT') > 0)
    and  status = '5') a
   where a.sql !='';
insert overwrite table json_job_sql_table partition(ds) select * from json_job_sql_table where sql!='' and ds between '20190301' and '20190324';



--张一鹏的表 存放所有解析过的sql以及plan
-- job_plan
-- | Field           | Type       | Label | Comment                                     |
-- +------------------------------------------------------------------------------------+
-- | project_name    | string     |       |                                             |
-- | sql             | string     |       |                                             |
-- | logical_plan    | string     |       |                                             |
-- | analyzed_plan   | string     |       |                                             |
-- | optimized_plan  | string     |       |                                             |
-- +------------------------------------------------------------------------------------+
-- | Partition Columns:                                                                 |
-- +------------------------------------------------------------------------------------+
-- | ds              | string     |                                                     |
-- +------------------------------------------------------------------------------------+

--job_sql_table  从m_instance 中过滤出所有的DML语句 包含（insert select） 目前只包含20190125日的数据

--创建只含json的表 json_job_sql_table;
-- +------------------------------------------------------------------------------------+
-- | Field           | Type       | Label | Comment                                     |
-- +------------------------------------------------------------------------------------+
-- | project_name    | string     |       |                                             |
-- | sql             | string     |       |                                             |
-- +------------------------------------------------------------------------------------+
-- | Partition Columns:                                                                 |
-- +------------------------------------------------------------------------------------+
-- | ds              | string     |                                                     |
-- +------------------------------------------------------------------------------------+
--用于存放含json的sql表
create table josn_job_sql_table like job_sql_table;
ALTER TABLE josn_job_sql_table rename to json_job_sql_table;
insert overwrite table json_job_sql_table partition(ds) select project_name,sql,ds from job_sql_table where ds = '20190125' and instr(sql,'get_json_object',0)>0;
--删除某个分区
ALTER TABLE json_job_plan_table  DROP IF EXISTS PARTITION(ds='20190124');
ALTER TABLE job_plan_table rename to json_job_plan_table;  --存放了解析出analyzedPlan的sql 以及计划 从json_job_sql_table中解析出来       69000/79000 = 86%通过率
select  analyzed_plan from json_job_plan_table where ds = '20190125' and instr(analyzed_plan, '...')>0 limit 1;
--模糊查询表
show tables like '*meta*';

create table if not exists json_access_frequency(
    project_name STRING,
    table_name STRING,
    col_name STRING,
    json_path STRING,
    json_path_count BIGINT,
    frequency DOUBLE
)partitioned by (ds STRING)
lifecycle 1111;

select json_path_count, count(*) as kind_summary, (cast(count(*) as double)/(select count(*) from json_access_frequency where ds = '20190125')) as ratio  from json_access_frequency where ds = '20190125' group by  json_path_count limit 1;

select count(1) from (select * from json_access_frequency where ds = '20190125') a join (select * from json_access_frequency where ds = '20190126') b on (a.json_path = b.json_path and a.project_name = b.project_name and a.col_name = b.col_name and a.table_name = b.table_name) join (select * from json_access_frequency where ds = '20190127') c on (c.json_path = b.json_path and c.project_name = b.project_name and c.col_name = b.col_name and c.table_name = b.table_name) where a.json_path_count >5 and a.json_path_count<=10;

select *  from json_access_frequency  where ds = '20190125' order by json_path_count desc limit 10;

select count(*) from json_access_frequency where json_path_count > 1 and json_path_count< 5 and ds = '20190126';

select count(*) from json_access_frequency where json_path_count = 1 and ds = '20190126';


select sum(json_path_count) as colCount from json_access_frequency  where ds = '20190125'  group by project_name,table_name, col_name order by colCount desc;


create table if not exists single_job_json_access_frequency(
    job_id STRING,
    project_name STRING,
    table_name STRING,
    col_name STRING,
    json_path STRING,
    json_path_count BIGINT,
    frequency DOUBLE
)partitioned by (ds STRING)
lifecycle 1111;

create table  if not exists table_raw_meta(
    project_name STRING COMMENT 'project名称',
    name               STRING  COMMENT '表名',
    type  STRING  COMMENT '类型，取值：TABLE/PARTITION/VIEW',
    raw_meta           STRING  COMMENT '原始的Meta信息，格式为JSON'
)
comment 'type 为 TABLE的表元信息'
partitioned by (ds STRING);

select raw_meta from table_raw_meta  where ds = '20190125' and project_name = 'alimama_ibrand' and name = 'ods_openssp_pubcontrol_x_taobao_filter_converted';
insert overwrite table table_raw_meta partition(ds) select project_name, name, type, raw_meta,ds from meta.m_table where (type='TABLE' or type='VIEW')  and ds = '20190125';

desc table_raw_meta partition(ds='20190125');

tunnel download   -rd "\n" -threads 1 -fd '@@'  odps_public_dev.table_raw_meta/ds='20190125' /Users/guyue/Downloads/user_json_query/table_meta.txt;
-- Function Cast 问题： 目前怀疑是动态注册时的多线程问题， 已经加锁，待检验效果。 =》 确定是由于函数多态问题，现在的方案是，注册完之后，获取完Func，直接remove掉
-- lateral View UDTF 问题， 因为Generator 无法注册， 故无解
--  聚合函数问题:  在duumy的情况下 不执行SessionCatalog 中的 checkAnalysis, 以保证动态注册的类跳过聚合函数检查

--  as ??id error! todo!!! 改了SqlBase g4  支持identifier有? 但是还是无法handle其他的？情况

-- insert overwrite table job_sql_table partition(ds='20190125') select project_name,regexp_replace(sql,'LIFECYCLE\\s+\\d+','',0) from job_sql_table where ds='20190125';
-- insert overwrite table job_sql_table partition(ds='20190125') select project_name,regexp_replace(sql,'lifecycle\\s+\\d+','',0) from job_sql_table where ds='20190125';

-- insert overwrite table job_sql_table partition(ds='20190125') select project_name,regexp_replace(sql,'\\&gt;','>',0) from job_sql_table where ds='20190125';

-- insert overwrite table job_sql_table partition(ds='20190125') select project_name,regexp_replace(sql,'\\&lt;','<',0) from job_sql_table where ds='20190125';

-- insert overwrite table job_sql_table partition(ds='20190125') select project_name,regexp_replace(sql,':','_',0) from job_sql_table where ds='20190125';

-- 建表语句
create table if not exists user_json_detail(
    project_name STRING COMMENT 'project名称',
    id           STRING COMMENT 'odps instance id',
    job_name     STRING COMMENT 'instance所属的odps job名称',
    owner_kp     STRING COMMENT 'instance owner的kp，比如：1059741237754411',
    owner_name   STRING COMMENT 'instance owner的云账号名称',
    status       STRING COMMENT 'instance的状态, 0:Ready, 1:Waiting, 2:Running, 3:Suspended, 4:Failed, 5:Terminated, 6:cancelled, 7,8,9,10:内部状态等同于Running, 11:Unkown',
    tasks_num    BIGINT COMMENT 'task的个数',
    start_time   BIGINT COMMENT 'instance启动时间',
    end_time     BIGINT COMMENT 'instance结束时间',
    source_xml   STRING COMMENT 'instance对应的xml源码，去除了敏感信息，运行期的',
    json_count   BIGINT COMMENT 'get_json_object 出现的次数'
)
comment '用户使用json统计表'
partitioned by (ds STRING);
-- clustered by (owner_name,json_count)  into 10 BUCKETS;

-- 修改表的lifecycle
alter table user_json_detail set lifecycle 1111;

-- 从meta.m_instance中查询
select job_name from meta.m_instance where ds = '20190122' and (regexp_count(source_xml,'get_json_object') + regexp_count(source_xml,'GET_JSON_OBJECT'))>10;

-- 收集指定日期数据
alter table user_json_detail add partition (ds='20190122')
from meta.m_instance
insert overwrite table user_json_detail partition(ds ='20190122')
    select project_name,id,job_name,owner_kp,owner_name,status,tasks_num,start_time,end_time,source_xml,
    (regexp_count(source_xml,'get_json_object') + regexp_count(source_xml,'GET_JSON_OBJECT'))
    where ds = '20190122' and (regexp_count(source_xml,'get_json_object') + regexp_count(source_xml,'GET_JSON_OBJECT'))>0
    and status = '5';

-- 收集一段范围日期数据，采用动态分区
insert overwrite table user_json_detail partition(ds)
    select project_name,id,job_name,owner_kp,owner_name,status,tasks_num,start_time,end_time,source_xml,
    (regexp_count(source_xml,'get_json_object') + regexp_count(source_xml,'GET_JSON_OBJECT')),ds
    from meta.m_instance
    where ds between '20190101' and '20190125' and (regexp_count(source_xml,'get_json_object') + regexp_count(source_xml,'GET_JSON_OBJECT'))>0
    and status = '5';


--
insert overwrite table user_json_detail partition(ds ='20181231')
    select project_name,id,job_name,owner_kp,owner_name,status,tasks_num,start_time,end_time,source_xml,
    (regexp_count(source_xml,'get_json_object') + regexp_count(source_xml,'GET_JSON_OBJECT'))
    from meta.m_instance
    where ds = '20181231' and (regexp_count(source_xml,'get_json_object') + regexp_count(source_xml,'GET_JSON_OBJECT'))>0
    and status = '5';
-- 查询语句
select project_name, id, job_name, owner_kp, owner_name,status,tasks_num,start_time,end_time,json_count from user_json_detail where ds='20190122' limit 10;

--查询分区信息
SHOW PARTITIONS user_json_detail;
desc user_json_detail partition(ds='20190120');

--删除分区
alter table user_json_detail drop partition (ds='20180121');
alter table user_json_detail drop partition (ds='20180122');

--增加一列用于记录每个source_xml，json调用的统计
ALTER TABLE user_json_detail add columns(repeated_path_count:STRING);


select ds, count(*) from meta.m_instance  where ds = '$targetDay' and status='5' and (source_xml rlike '(?s).*<Query>.*(select|SELECT|INSERT|insert).*</Query></SQL>.*') group by ds

--Tunnel 拉取数据
tunnel download -limit 10 -ni "null" -rd "\n" -threads 1 -cn source_xml odps_public_dev.user_json_detail/ds='20190122' /Users/guyue/Downloads/user_json_query/user_json_20190122;

tunnel download   -limit 5 -fd "@@" -rd "\n" -cn project_name,sql,analyzed_plan odps_public_dev.json_job_plan_table/ds='20190125' /Users/guyue/Downloads/user_json_query/json_job_plan_table/20190125;

--create as

create table if not exists test_json
as
select project_name, json_count, ds from user_json_detail where ds between '20190101' and '20190123';



--dml_table 有20190101~20190110的DML语句，从meta.m_instance中过滤出来的
--meta_temp 有20190125这一天 type 为 TABLE的raw_meta



--查询一段时间包含Json的作业总数
select ds, count(*) from meta.m_instance where ds between '20190101' and '20190125' and (regexp_count(source_xml,'get_json_object') + regexp_count(source_xml,'GET_JSON_OBJECT'))>0 group by ds;

-- 给每天不存在的json_path补0
create table  period_full_json_path as select distinct project_name,table_name,col_name,json_path from json_access_frequency where ds between '20181201' and '20181231';
insert overwrite period_full_json_path select distinct project_name,table_name,col_name,json_path from json_access_frequency where ds between '$start_day' and '$end_day';
create table json_access_frequency_with_zero like json_access_frequency;

insert into table json_access_frequency_with_zero partition(ds='20181201') select a.project_name,a.table_name,a.col_name,a.json_path,0,0 from (select * from period_full_json_path)a left anti join (select * from json_access_frequency where ds = '20181201') b on a.project_name = b.project_name and a.table_name = b.table_name and a.col_name = b.col_name and a.json_path = b.json_path;

insert into table json_access_frequency_with_zero partition(ds = '20181201') select * from json_access_frequency where ds ='20181201';

create table lstm_train_set as select distinct concat(project_name,'@',table_name,'@',col_name,'@',json_path) as full_json_path, json_count, ds from json_access_frequency_with_zero where ds between '20181201' and '20181202' order by full_json_path limit 10;

insert overwrite table lstm_train_set select  distinct concat(trim(project_name),'@',trim(table_name),'@',trim(col_name),'@',trim(json_path)) as full_json_path, json_path_count, ds from json_access_frequency_with_zero where ds between '20181201' and '20181230' order by full_json_path asc ,ds asc limit 1043754
insert overwrite table lstm_train_set select distinct concat(project_name,'@',table_name,'@',col_name,'@',json_path) as full_json_path, json_path_count, ds from json_access_frequency_with_zero where ds between '20181201' and '20181202' distribute  by full_json_path , sort by full_json_path asc, ds asc limit 100

tunnel download   -rd "\n" -threads 1 -fd '^^'  odps_public_dev.lstm_train_set /home/admin/guyue/lstm_train_set.txt;

tunnel download   -rd "\n" -threads 1 -fd '^^'  odps_public_dev.lstm_predict_set /home/admin/guyue/lstm_predict_set.txt;


-- ==> For changes to take effect, close and re-open your current shell. <==

-- If you'd prefer that conda's base environment not be activated on startup,
--    set the auto_activate_base parameter to false:

-- conda config --set auto_activate_base false



-- 创建测试集
create table  period_full_json_path_predict_set as select distinct project_name,table_name,col_name,json_path from json_access_frequency where ds between '20190201' and '20190302';
alter table period_full_json_path_predict_set set lifecycle 365;
insert overwrite period_full_json_path_predict_set select distinct project_name,table_name,col_name,json_path from json_access_frequency where ds between '$start_day' and '$end_day';

create table json_access_frequency_with_zero_predict_set like json_access_frequency;
alter table json_access_frequency_with_zero_predict_set set lifecycle 365;
insert overwrite table lstm_predict_set as  select distinct concat(trim(project_name),'@',trim(table_name),'@',trim(col_name),'@',trim(json_path)) as full_json_path, json_path_count, ds from json_access_frequency_with_zero_predict_set where ds between '20190201' and '20190302' order by full_json_path asc ,ds asc;

tunnel download   -rd "\n" -threads 1 -fd '^^'  odps_public_dev.lstm_predict_set /home/admin/guyue/lstm_predict_set.txt;


select  distinct concat(trim(project_name),'@',trim(table_name),'@',trim(col_name),'@',trim(json_path)) as full_json_path, json_path_count, ds from json_access_frequency_with_zero_predict_set where ds between '20190201' and '20190302' and col_name = 'ext_info' and json_path = 'loan_term' order by full_json_path asc ,ds asc;
select * from (select  distinct concat(trim(project_name),'@',trim(table_name),'@',trim(col_name),'@',trim(json_path)) as full_json_path, json_path_count, ds from json_access_frequency_with_zero_predict_set where ds between '20190201' and '20190302'  order by full_json_path asc ,ds asc) a where col_name = 'ext_info' and json_path = 'loan_term';


insert overwrite table json_access_frequency partition(ds)
 select trim(project_name),
  trim(table_name),
  trim(col_name),
  trim(json_path),
  json_path_count,
  frequency, ds
 from json_access_frequency where ds between '20181201' and '20190324';


insert overwrite table json_access_frequency partition(ds)
 select project_name,
  table_name,
  col_name,
  json_path,
  sum(json_path_count),
  avg(frequency), ds
 from json_access_frequency where ds between '20181201' and '20190324' group by project_name, table_name, col_name,json_path,ds;



 create table lstm_verify_set as select distinct concat(trim(project_name),'@',trim(table_name),'@',trim(col_name),'@',trim(json_path)) as full_json_path, json_path_count, ds from json_access_frequency_with_zero_predict_set where ds ='20190303';

tunnel download   -rd "\n" -threads 1 -fd '^^'  odps_public_dev.lstm_verify_set /home/admin/guyue/lstm_verify_set.txt;

