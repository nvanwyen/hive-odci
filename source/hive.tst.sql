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

-- simple query
select * from table( hive_q( 'select cust_id, last_name, first_name from cust',
                             null, 
                             null ) );

-- create a runtime only bind list (q-string example)
select * from table( hive_q( q'[select cust_id, 
                                       last_name, 
                                       first_name 
                                  from cust 
                                 where last_name = ?
                                   and cust_id between ? and ?
                                 order by cust_id asc]',
                             hive_binds( hive_bind( 'Hamada', 9 /* type_string */, 1 /* ref_in */ ),
                                         hive_bind( 1144011,  5 /* type_long */,   1 /* ref_in */ ),
                                         hive_bind( 1337250,  5 /* type_long */,   1 /* ref_in */ ) ) ) );

-- create a view
create or replace view hive.cust ( cust_id, last_name, first_name ) as
select * from table( hive_q( 'select cust_id, last_name, first_name from cust' ) )
/

set linesize 80
desc hive.cust

exit
