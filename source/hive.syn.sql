--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.syn.sql
--

--
prompt ... running hive.syn.sql

--
create or replace public synonym hive_q for hive.hive_q;
create or replace public synonym hive_t for hive.hive_t;

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
create or replace public synonym dbms_hive for hive.dbms_hive;

--
create or replace public synonym dba_hive_params for hive.dba_hive_params;

--
create or replace public synonym dba_hive_filters for hive.dba_hive_filters;
create or replace public synonym dba_hive_filter_privs for hive.dba_hive_filter_privs;

--
create or replace public synonym dba_hive_log for hive.dba_hive_log;

--
-- ... done!
--
