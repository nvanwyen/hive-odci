--------------------------------------------------------------------------------
--
-- 2016-06-07, NV - hive.prm.sql
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

-- online index rebuild (move_ts)
grant create any index to hive;
grant create any table to hive;

--
-- ... done!
--
