--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - remove_hive.sql
--

/*
  Hive-ODCI - Copyright (C) 2006-2016 Metasystems Technologies Inc. (MTI)
  Nicholas Van Wyen
  
  This library is free software; you can redistribute it and/or modify it 
  under the terms of the GNU Lesser General Public License as published by 
  the Free Software Foundation; either version 2.1 of the License, or (at 
  your option) any later version.
  
  This library is distributed in the hope that it will be useful, but 
  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
  or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
  License for more details.
  
  You should have received a copy of the GNU Lesser General Public License 
  along with this library; if not, write to the
  
                  Free Software Foundation, Inc.
                  59 Temple Place, Suite 330,
                  Boston, MA 02111-1307 USA
*/

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
drop public synonym hive_remote;
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
drop public synonym user_hive_params;
drop public synonym user_hive_filters;
drop public synonym user_hive_filter_privs;

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
