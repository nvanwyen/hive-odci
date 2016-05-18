--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.vws.sql
--

--
prompt ... running hive.vws.sql

--
alter session set current_schema = hive;

--
create or replace view dba_hive_params
as
select name,
       value
  from param$
 order by name;

--
create or replace view dba_hive_filters
as
select key,
       seq,
       type,
       value
  from filter$
 order by key,
          seq;

--
create or replace view dba_hive_log
as
select stamp,
       type,
       text
  from log$
 order by stamp;

--
-- ... done!
--
