--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - attrs.typ.sql
--

--
prompt ... running attrs.typ.sql

--
alter session set current_schema = hive;

--
create or replace type metadata_t is object
(
    typecode  number,
    precision integer,
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
    attr_name varchar2( 128 )
);
/

--
create or replace type column_t as object
(
    typecode     number,
    v2_column    varchar2( 32767 ),
    num_column   number,
    date_column  date,
    blob_column  blob,
    clob_column  clob,
    raw_column   raw( 32767 ),
    raw_error    number,
    raw_length   number,
    ids_column   interval day to second,
    iym_column   interval year to month,
    ts_column    timestamp,
    tstz_column  timestamp with time zone,
    tsltz_column timestamp with local time zone,
    cvl_offset   number,
    cvl_length   number
);
/

--
show errors

--
create or replace type row_t as table of column_t;
/

--
show errors

--
-- ... done!
--
