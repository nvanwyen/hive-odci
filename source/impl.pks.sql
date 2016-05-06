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
    procedure initialize( obj out nocopy anytype );
    procedure finalize( obj in out nocopy anytype );

    --
    procedure attribute( obj   in out nocopy anytype,
                         name  in            varchar2,
                         code  in            pls_integer,
                         prec  in            pls_integer,
                         scale in            pls_integer,
                         len   in            pls_integer,
                         csid  in            pls_integer,
                         csfrm in            pls_integer,
                         attr  in            anytype default null);

    --
    procedure clone( trg in out nocopy anytype,
                     src in            anytype );

end impl;
/

show errors

--
-- ... done!
--
