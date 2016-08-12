--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - remove_hive.sql
--

/*
 Copyright (c) 2016, Metasystems Technologies Inc (MTI), Nicholas Van Wyen
 All rights reserved.
 
 Redistribution and use in source and binary forms, with or without 
 modification, are permitted provided that the following conditions are met:
 
 1. Redistributions of source code must retain the above copyright notice, this
    list of conditions and the following disclaimer.
 
 2. Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.
 
 3. Neither the name of the copyright holder nor the names of its contributors
    may be used to endorse or promote products derived from this software
    without specific prior written permission.
 
 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
