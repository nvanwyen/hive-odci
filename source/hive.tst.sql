set linesize 160
set pagesize 50000

alter session set current_schema = hive;

--
set serveroutput on
exec dbms_java.set_output( 1000000 );

-- exec debug_clear;

--
-- select * from table( hive.query( 'select * from movies' ) );
-- select * from table( hive.query( 'select * from cust' ) );

-- select * from table( hive.query( 'select cust_id, last_name, first_name from cust' ) );
-- select * from table( hive.query( 'select last_name, first_name from cust' ) );

col cust_id    for 9999990
col last_name  for a30
col first_name for a30

select * from table( hive_q( 'select cust_id, last_name, first_name from cust' ) ) ;


------------ create or replace view hive.cust ( cust_id, last_name, first_name ) as
------------ select * from table( hive_q( 'select cust_id, last_name, first_name from cust' ) )
------------ /
------------ 
------------ set linesize 80
------------ desc hive.cust
------------ 
------------ -- remove previous bind lins and save new bind variables for later use
------------ exec hive_binding.clear( 'cust.id' );
------------ exec hive_binding.append( 'cust.id', '1037854', hive_binding.scope_in, hive_binding.type_long );
------------ 
------------ -- use a pre-saved bind list (from key above)
------------ select * from table( hive_q( 'select cust_id, last_name, first_name from cust where cust_id = ?',
------------                              hive_binding.get( 'cust.id' ) ) );
------------ 
------------ -- create a runtime only bind list (q-string example)
------------ select * from table( hive_q( q'[select cust_id, 
------------                                        last_name, 
------------                                        first_name 
------------                                   from cust 
------------                                  where last_name like ? 
------------                                    and first_name like ?]',
------------                              hive_binds( hive_bind( 'A%', 1 /* ref_in */, 9 /* type_string */ ),
------------                                          hive_bind( 'J%', 1 /* ref_in */, 9 /* type_string */ ) ) ) );

exit
