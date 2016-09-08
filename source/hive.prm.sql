--------------------------------------------------------------------------------
--
-- 2016-06-07, NV - hive.prm.sql
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
prompt ... running hive.prm.sql

--
grant resource to hive;

--
grant create any operator to hive;
grant execute any operator to hive;
grant unlimited tablespace to hive;

--
grant select on sys.user$                   to hive with grant option;
grant select on sys.resource_group_mapping$ to hive with grant option;
grant select on sys.user_astatus_map        to hive with grant option;

--
grant select on dba_tab_columns       to hive;
grant select on dba_views             to hive;
grant select on dba_constraints       to hive;
grant select on dba_tab_partitions    to hive;
grant select on dba_tab_subpartitions to hive;
grant select on dba_triggers          to hive;
grant select on dba_role_privs        to hive;

grant select on dba_free_space        to hive;
grant select on dba_data_files        to hive;
grant select on dba_segments          to hive;
grant select on dba_ts_quotas         to hive;
grant select on dba_tables            to hive;
grant select on dba_indexes           to hive;


--
grant execute on dbms_sql      to hive;
grant execute on dbms_session  to hive;
grant execute on dbms_standard to hive;

--
grant javasyspriv to hive;
grant javadebugpriv to hive;

--
-- ... done!
--
