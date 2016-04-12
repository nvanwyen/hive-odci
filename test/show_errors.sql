
set linesize 160
set pagesize 50000

col name a30 head "name"
col text a80 head "text" word_wrap

select name,
       text
  from all_errors
 where owner = 'HIVE';
