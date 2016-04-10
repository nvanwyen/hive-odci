--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - hive.tys.sql
--

--
prompt ... running hive.tys.sql

--
alter session set current_schema = hive;

-- generic ANYTYPE abstraction for Hive queries

--
create or replace type hive_t as object
(
    type anytype, -- transient record type

    static function odcitabledescribe( type  out anytype,
                                       stmt  in  varchar2 ) return number,

    static function odcitableprepare( this  out hive_t,
                                      info  in  sys.odcitabfuncinfo,
                                      stmt  in  varchar2 ) return number,

    static function odcitablestart( this in out hive_t,
                                    stmt in     varchar2 ) return number,

    member function odcitablefetch( this  in out hive_t,
                                    rows  in     number,
                                    type  out    anydataset ) return number,

    member function odcitableclose( this in hive_t ) return number
);
/

show errors

--
-- ... done!
--
