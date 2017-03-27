set linesize 160
set pagesize 50000

--
alter session set current_schema = hive;

--
set serveroutput on
exec dbms_java.set_output( 1000000 );

exec dbms_hive.purge_log;
exec hive_remote.session_param( 'log_level', 31 );

--
set linesize 160
set pagesize 50000

--
col name  for a32 word_wrap
col value for a95 word_wrap

--
select name,
       value
  from user_hive_params
 where name not in ( 'application', 'license' )
/



--
col id_nbr for a30

-- this one is broken ... get stack trace
--
select * from table( hive_q( 'select id_nbr
                                from plc_pnr
                               where pnr_clob_obj like ?',
                             hive_binds( hive_bind( nvl( sys_context( 'hiveenv', 'pnr_clob_obj' ),  '%' ) ) ),
                             null ) );
/* *** results in error ***

    select * from table( hive_q( 'select id_nbr
    *
    ERROR at line 1:
    ORA-29532: Java call terminated by uncaught Java exception: 
               java.sql.SQLException: [DataDirect][Hive JDBC Driver]Invalid parameter binding(s).
    ORA-06512: at "HIVE.HIVE_T", line 38
    ORA-06512: at line 4

*** */

--
exec dbms_hive.purge_log;

-- this one is also broken ... get stack trace
--
select * from table( hive_q( 'select id_nbr
                                from plc_pnr
                               where pnr_clob_obj like ?',
                             hive_binds( hive_bind( '%test1%' ) ),
                             null ) );
/* *** results in error ***

    select * from table( hive_q( 'select id_nbr
    *
    ERROR at line 1:
    ORA-29532: Java call terminated by uncaught Java exception: 
               java.sql.SQLException: [DataDirect][Hive JDBC Driver]Invalid parameter binding(s).
    ORA-06512: at "HIVE.HIVE_T", line 38
    ORA-06512: at line 4

*** */

--
exec dbms_hive.purge_log;

-- this one works ... why ... get log data
--
select * from table( hive_q( 'select id_nbr
                                from plc_pnr
                               where pnr_clob_obj like ?',
                                hive_binds( hive_bind( '%test1%' ) ),
                             null ) );
/* *** results in error ***

    select * from table( hive_q( 'select id_nbr
    *
    ERROR at line 1:
    ORA-29532: Java call terminated by uncaught Java exception: 
               java.sql.SQLException: [DataDirect][Hive JDBC Driver]Invalid parameter binding(s).
    ORA-06512: at "HIVE.HIVE_T", line 38
    ORA-06512: at line 4

*** */

--
col stamp for a28
col type  for a12
col text  for a95 word_wrap

--
select stamp,
       decode( type, 0, 'none',
                     1, 'error',
                     2, 'warn',
                     4, 'info',
                     8, 'trace',
                        'unknown' ) type,
       text
  from dba_hive_log
 order by stamp
/

--
exit

