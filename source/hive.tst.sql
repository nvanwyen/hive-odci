set linesize 160
set pagesize 50000

alter session set current_schema = hive;

--
set serveroutput on
exec dbms_java.set_output( 1000000 );

exec debug_clear;

--
-- select * from table( hive_q( 'select * from movies' ) );
-- select * from table( hive_q( 'select * from cust' ) );

-- select * from table( hive_q( 'select cust_id, last_name, first_name from cust' ) );
-- select * from table( hive_q( 'select last_name, first_name from cust' ) );

col cust_id    for 9999990
col last_name  for a30
col first_name for a30
select * from table( hive_q( 'select cust_id, last_name, first_name from cust' ) );

-- create table hive.cust as
-- select * from table( hive_q( 'select cust_id, last_name, first_name from cust' ) );
-- 
-- select * from hive.cust;


-- @@hive.dbx.sql
exit
