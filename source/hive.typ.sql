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
    static function ODCITableStart( ctx out hive_t,
                                    stm in varchar2 ) return number as
    language java
    name 'oracle.mti.hive.ODCITableStart( oracle.sql.STRUCT[], java.lang.String ) return java.math.BigDecimal',

    --
    static function ODCITableDescribe( typ out anytype,
                                       stm in varchar2 ) return number as
    language java
    name 'oracle.mti.hive.ODCITableDescribe( oracle.sql.STRUCT[], java.lang.String ) return java.math.BigDecimal',

    --
    member function ODCITableFetch( self in out hive_t,
                                    max  in     number,
                                    rws  out    anydataset ) return number as
    language java
    name 'oracle.mti.hive.ODCITableFetch( java.math.BigDecimal, oracle.sql.Array[] ) return java.math.BigDecimal',

    --
    member function ODCITableClose( self in hive_t ) return number as
    language java
    name 'oracle.mti.hive.ODCITableFetch( oracle.sql.STRUCT[] ) return java.math.BigDecimal'
);
/

show errors

--
-- ... done!
--
