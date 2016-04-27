--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - hive.tys.sql
--

--
prompt ... running hive.tys.sql

--
alter session set current_schema = hive;

--
create or replace type hive_t as object 
(
    --
    key integer,

    --
    static function odcitablestart( ctx out hive_t,
                                    stm in varchar2 ) return number as
    language java
    name 'oracle.mti.hive.table_start( oracle.sql.struct[], java.sql.resultset ) return java.math.bigdecimal',

    --
    member function odcitablefetch( self   in out hive_t,
                                    nrows  in     number,
                                    rws    out    anydataset ) return number as
    language java
    name 'oracle.mti.hive.table_fetch( oracle.sql.STRUCT[], java.math.bigdecimal, oracle.sql.array[] ) return java.math.bigdecimal',

    --
    member function odcitableclose( self in hive_t ) return number as
    language java
    name 'oracle.mti.hive.table_close( oracle.sql.STRUCT[] ) return java.math.bigdecimal'

);
/
