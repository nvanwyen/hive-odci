set linesize 160
set pagesize 50000

--
alter session set current_schema = hive;

--
set serveroutput on
exec dbms_java.set_output( 1000000 );

--
col cust_id    for 9999990
col last_name  for a30
col first_name for a30
col total      for 9,999,990

--
select * from table( hive_q( 'select cust_id, last_name, first_name from cust',
                             null, 
                             null ) );

--
select * from table( hive_q( 'select last_name, count(0) total from cust group by last_name order by total desc limit 10',
                             null,
                             null ) );

-- create a runtime only bind list (q-string example)
select * from table( hive_q( q'[select cust_id, 
                                       last_name, 
                                       first_name 
                                  from cust 
                                 where last_name = ?]',
                             hive_binds( hive_bind( 'Hamada', 9 /* type_string */, 1 /* ref_in */ ) ) ) );

--
create or replace view hive.cust ( cust_id, last_name, first_name ) as
select * from table( hive_q( 'select cust_id, last_name, first_name from cust' ) )
/

exit
