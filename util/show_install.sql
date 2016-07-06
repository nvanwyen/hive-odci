--------------------------------------------------------------------------------
--
-- 2016-06-30, NV - show_install.sql
--

--
set linesize 160
set pagesize 50000

--
col name  for a30 word_wrap
col value for a40 word_wrap

--
select name,
       system_value value
  from dba_hive_params
 where name in ( 'application', 'version' )
 order by name
/

--
col name     for a30 
col type     for a16
col line     for 999,990
col position for 999,990
col text     for a55 word_wrap

--
select name,
       type,
       line,
       position,
       text
  from all_errors
 where owner = 'HIVE'
 order by name,
          type,
          line,
          position
/

--
-- ... done!
--
