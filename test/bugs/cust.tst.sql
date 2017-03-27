--
column log new_value log noprint;

--
set termout off;
select 'cust.tst.' 
    || to_char( sysdate, 'YYYYMMDDHH24MISS' ) 
    || '.log' log
  from dual;
set termout on;

--
spool &&log append

set echo on
set trimspool on
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
select count(0) from table( hive_q( 'select * from cust where last_name like ?', hive_binds( hive_bind( '%A%' ) ) ) );

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
spool off
exit

