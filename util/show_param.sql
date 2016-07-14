--------------------------------------------------------------------------------
--
-- 2016-06-30, NV - show_param.sql
--

-- show the HIVE-ODCI parameters

set linesize 160
set pagesize 50000

--
col name          for a32 word_wrap
col session_value for a45 word_wrap
col system_value  for a45 word_wrap

--
select name,
       session_value,
       system_value
  from dba_hive_params
/

--
-- ...done!
--
