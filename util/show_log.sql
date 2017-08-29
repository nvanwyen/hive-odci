--------------------------------------------------------------------------------
--
-- 2016-06-30, NV - show_log.sql
--

-- show the HIVE-ODCI log

set linesize 160
set pagesize 50000

--
col stamp for a28
col name  for a16 trunc
col type  for 990
col tier  for a12
col text  for a75 word_wrap

--
select stamp,
       name,
       type,
       tier,
       text
  from dba_hive_log
 order by stamp
/

--
-- ...done!
--
