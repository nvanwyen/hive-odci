--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.syn.sql
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
prompt ... running hive.syn.sql

--
create or replace public synonym hive_q for hive.hive_q;
create or replace public synonym hive_t for hive.hive_t;

--
create or replace public synonym hive_remote  for hive.remote;

--
create or replace public synonym hive_bind    for hive.bind;
create or replace public synonym hive_binds   for hive.binds;
create or replace public synonym hive_binding for hive.binding;

--
create or replace public synonym hive_attribute  for hive.attribute;
create or replace public synonym hive_attributes for hive.attributes;

--
create or replace public synonym hive_data    for hive.data;
create or replace public synonym hive_records for hive.records;

--
create or replace public synonym hive_connection for hive.connection;

--
create or replace public synonym hive_hint for hive.hive_hint;

--
create or replace public synonym dbms_hive for hive.dbms_hive;

--
create or replace public synonym dba_hive_params for hive.dba_hive_params;

--
create or replace public synonym dba_hive_filters for hive.dba_hive_filters;
create or replace public synonym dba_hive_filter_privs for hive.dba_hive_filter_privs;

--
create or replace public synonym dba_hive_log for hive.dba_hive_log;

--
create or replace public synonym user_hive_params for hive.user_hive_params;

--
create or replace public synonym user_hive_log for hive.user_hive_log;
create or replace public synonym user_hive_filters for hive.user_hive_filters;
create or replace public synonym user_hive_filter_privs for hive.user_hive_filter_privs;

--
-- ... done!
--
