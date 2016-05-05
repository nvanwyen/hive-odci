
alter session set current_schema = hive;

--
set serveroutput on
exec dbms_java.set_output( 1000000 );

--
-- select * from table( hive_q( 'select * from movies' ) );
select * from table( hive_q( 'select * from cust' ) );
