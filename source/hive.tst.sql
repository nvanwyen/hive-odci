
alter session set current_schema = hive;

--
set serveroutput on
exec dbms_java.set_output( 1000000 );

--
-- select * from table( hive_q( 'select * from movies' ) );
-- select * from table( hive_q( 'select * from cust' ) );

-- select * from table( hive_q( 'select cust_id, last_name, first_name from cust' ) );
select * from table( hive_q( 'select last_name, first_name from cust' ) );

-- col nam for a30
-- col cod for 9990
-- col pre for 9990
-- col sca for 9990
-- col len for 9990
-- col csi for 9990
-- col csf for 9990
-- --
-- select * from hive.test$;
