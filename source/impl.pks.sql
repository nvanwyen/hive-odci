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

    --
    procedure connection( con in session );

    --
    function connection return session;

    --
    procedure begin_create( rec in out anytype );

    --
    procedure add_attribute( name  in     varchar2,
                             type  in     number,
                             prec  in     number,
                             scale in     number,
                             len   in     number,
                             chrid in     number,
                             chrfm in     number,
                             rec   in out anytype );

    --
    procedure end_create( rec in out anytype );

    --
    procedure swap_anytype( rec1 in     anytype,
                            rec2 in out anytype );

    --
    procedure row_instance( rec in out anytype );

    --
    procedure row_piecewise( rec in out anytype );

end impl;
/

show errors

--
-- ... done!
--
