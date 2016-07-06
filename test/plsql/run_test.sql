--------------------------------------------------------------------------------
--
--
--


--
alter session set current_schema = hive;

set serveroutput on;
call dbms_java.set_output( 1000000 );

exec hive_test;

