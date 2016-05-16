set linesize 160
set pagesize 50000

alter session set current_schema = hive;

--
prompt ... debug SYS.ANYTYPE info
--
col txt          for a30      head "txt"
col code         for 9990     head "code"
col prec         for 9990     head "prec"
col scale        for 9990     head "scale"
col len          for 9990     head "len"
col csid         for 9990     head "csid"
col csfrm        for 9990     head "csfrm"
col schema_name  for a10      head "schema_name"
col type_name    for a10      head "type_name"
col version      for a10      head "version"
col attr_count   for 9990     head "attr_count"
--
select *
  from dbg$info$
 order by 1;

--
prompt ... debug SYS.ANYTYPE attribute element info
--
col txt          for a30      head "txt"
col code         for 9990     head "code"
col idx          for 9990     head "idx"
col prec         for 9990     head "prec"
col scale        for 9990     head "scale"
col len          for 9990     head "len"
col csid         for 9990     head "csid"
col csfrm        for 9990     head "csfrm"
col attr_type    for a20      head "attr_type"
col attr_name    for a20      head "attr_name"
--
select *
  from dbg$info$elem$
 order by 1, 3;

--
prompt ... debug HIVE.ATTRIBUTES data
--
col txt          for a30      head "txt"
col idx          for 9990     head "idx"
col name         for a20      head "name"
col code         for 9990     head "code"
col prec         for 9990     head "prec"
col scale        for 9990     head "scale"
col len          for 9990     head "len"
col csid         for 9990     head "csid"
col csfrm        for 9990     head "csfrm"
--
select *
  from dbg$attribute$
 --order by 1, 3;

--
prompt ... debug HIVE.RECORDS data
--
col txt           for a20       head "txt"
col key           for 9990      head "key"
col num           for 9990      head "row"
col idx           for 9990      head "idx"
col code          for 9990      head "code"
col val_varchar2  for a25       head "varchar2" word_wrap
col val_number    for 999999990 head "number"
col val_date      for a9        head "date"
col val_timestamp for a9        head "timestamp"
col val_clob      for a9        head "clob"
col val_blob      for a9        head "blob"
--
select *
  from dbg$record$
 order by 1, 2, 3, 4;

--
prompt ... debug LOG data
--
col stamp        for a29       head "stamp"
col txt          for a30       head "txt" word_wrap
col msg          for a50       head "msg" word_wrap
--
select *
  from dbg$log$
 order by stamp;

