--------------------------------------------------------------------------------
--
-- 2016-06-30, NV - show_binds.sql
--

-- show the HIVE-ODCI bind filters

--
set linesize 160
set pagesize 50000

--
col key   for a30  word_wrap
col seq   for 9990 
col type  for a15
col scope for a12
col owner for a30
col value for a50  word_wrap

--
select key,
       seq,
       type,
       scope,
       owner,
       value
  from dba_hive_filters
 order by key,
          seq
/

--
-- ... done!
--
