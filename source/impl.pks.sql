--------------------------------------------------------------------------------
--
-- 2016-04-19, NV - impl.pks.sql
--

--
prompt ... running impl.pks.sql

--
alter session set current_schema = hive;

--
create or replace package impl as

    -- Internet Corporation for Assigned Names and Numbers (service)
    subtype service is binary_integer range 0 .. 65536; -- 16 bit (2^16)

    -- 
    type session is record ( host varchar2( 256 ),
                             port service,
                             user varchar2( 256 ),
                             pass varchar2( 256 ) );

    -- 
    procedure connection( usr in varchar2,
                          pwd in varchar2 );

    -- 
    procedure connection( hst in varchar2,
                          prt in service,
                          usr in varchar2,
                          pwd in varchar2 );

    --
    procedure connection( con in session );

    --
    function connection return session;

    --
    function sql_describe( stm in varchar2, atr out attributes ) return number as
    language java
    name 'oracle.mti.hive.SqlDesc( java.lang.String, oracle.sql.ARRAY[] ) return java.math.BigDecimal';

    --
    function sql_fetch( max in number, rws out records ) return number as
    language java
    name 'oracle.mti.hive.SqlFetch( java.math.BigDecimal, oracle.sql.ARRAY[] ) return java.math.BigDecimal';

end impl;
/

show errors

--
-- ... done!
--
