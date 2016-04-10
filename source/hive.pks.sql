--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.pks.sql
--

--
prompt ... running hive.pks.sql

--
alter session set current_schema = hive;

--
create or replace package hive as

    -- Internet Corporation for Assigned Names and Numbers (service)
    subtype service is binary_integer range 0 .. 65536; -- 16 bit (2^16)

    -- HQL metadata
    type metadata is record ( precision integer,
                              scale     integer,
                              length    integer,
                              csid      integer,
                              csfrm     integer,
                              schema    varchar2( 30 ),
                              type      anytype,
                              name      varchar2( 30 ),
                              version   varchar2( 30 ),
                              attr_cnt  integer,
                              attr_type anytype,
                              attr_name varchar2( 128 ),
                              typecode  integer );

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

    -- set session connection data
    procedure connection( con in session );

    -- get session connection data (password returned is intentionally null)
    function connection return session;

    --
    function query( stm in varchar2,
                    bnd in binds default null ) return anydataset pipelined using hive_t;

    --
    procedure dml( stm in varchar2,
                   bnd in binds default null );

    --
    procedure ddl( stm in varchar2 );

end hive;
/

show errors

--
-- ... done!
--
