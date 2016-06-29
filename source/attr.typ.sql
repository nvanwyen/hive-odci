--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - attrs.typ.sql
--

--
prompt ... running attrs.typ.sql

--
alter session set current_schema = hive;

-- column description, marries up to ANYTYPE.ADDATTR
create or replace type attribute is object
(
    name  varchar2( 256 ),
    code  number,
    prec  number,
    scale number,
    len   number,
    csid  number,
    csfrm number
);
/

-- array of column descriptions
create or replace type attributes as table of attribute;
/

-- column data, marries up to ANYDATA
create or replace type data as object
(
    code          number,   -- see also: attribute.code
    --
    val_varchar2  varchar2( 32767 ),
    val_number    number,
    val_date      date,
    val_timestamp timestamp,
    val_clob      clob,
    val_blob      blob
    -- other data type not yet supported
);
/

--
show errors

-- array of column data
create or replace type records as table of data;
/

--
show errors

-- 
create or replace type connection as object
(
    host varchar2( 256 ),
    port varchar2( 256 ),
    name varchar2( 256 ),
    pass varchar2( 256 ),
    kerb varchar2( 256 ) 
);
/

show errors

--
-- ... done!
--
