drop database if exists test;
create database test;
use test;

create table t1 (name varchar(25));
load data infile '$resources/load_data/load.csv' INTO TABLE `t1` FIELDS TERMINATED BY '\t' escaped by '' LINES TERMINATED BY '\n' PARALLEL 'TRUE';
select * from t1;
load data infile '$resources/load_data/load.csv' INTO TABLE `t1` FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' PARALLEL 'TRUE';
select * from t1;
load data infile '$resources/load_data/load.csv' INTO TABLE `t1` FIELDS TERMINATED BY '\t' escaped by '\\' LINES TERMINATED BY '\n' PARALLEL 'TRUE';
select * from t1;

drop table t1;
drop database test;