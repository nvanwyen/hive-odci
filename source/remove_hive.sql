--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - remove_hive.sql
--

--
set linesize 160
set pagesize 50000
set trimspool on

--
column logfile new_value logfile noprint;

--
set termout off;
select sys_context( 'userenv', 'db_name' ) 
    || '_remove_hive_odci.' 
    || to_char( sysdate, 'YYYYMMDDHH24MISS' ) 
    || '.log' logfile
  from dual;
set termout on;

--
spool &&logfile

--
prompt ... running remove_hive.sql

--
select current_timestamp "beginning removal"
  from dual;

--
drop user hive cascade;

--
drop public synonym hive_t;
drop public synonym hive_q;
drop public synonym hive_bind;
drop public synonym hive_binds;
drop public synonym hive_binding;
drop public synonym hive_attribute;
drop public synonym hive_attributes;
drop public synonym hive_data;
drop public synonym hive_records;
drop public synonym hive_connection;
drop public synonym dbms_hive;
drop public synonym dba_hive_params;
drop public synonym dba_hive_filters;
drop public synonym dba_hive_filter_privs;
drop public synonym dba_hive_log;

--
drop role hive_user;
drop role hive_admin;

--
select current_timestamp "completed removal"
  from dual;

--
spool off
exit

--
-- ... done!
--
