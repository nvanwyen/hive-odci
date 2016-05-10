--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - hive.typ.sql
--

--
prompt ... running hive.typ.sql

--
alter session set current_schema = hive;

-- /* *** debug stuff *** */
-- 
-- drop table test$;
-- 
-- create table test$
-- (
--     nam varchar2( 256 ),
--     cod number,
--     pre number,
--     sca number,
--     len number,
--     csi number,
--     csf number
-- );
-- 
-- create or replace procedure test_( a in attributes ) is
-- 
--     pragma autonomous_transaction;
-- 
-- begin
-- 
--     if ( a is null ) then
-- 
--         begin
-- 
--             delete from test$;
--             commit;
-- 
--             exception
--                 when others then
--                     rollback;
-- 
--         end;
-- 
--     else
-- 
--         for i in 1 .. a.count loop
-- 
--             begin
-- 
--                 insert into test$
--                 (
--                     nam,
--                     cod,
--                     pre,
--                     sca,
--                     len,
--                     csi,
--                     csf
--                 )
--                 values
--                 (
--                     a( i ).name,
--                     a( i ).code,  /* case when a( i ).code  = -1 then null else a( i ).code  end, */
--                     a( i ).prec,  /* case when a( i ).prec  = -1 then null else a( i ).prec  end, */
--                     a( i ).scale, /* case when a( i ).scale = -1 then null else a( i ).scale end, */
--                     a( i ).len,   /* case when a( i ).len   = -1 then null else a( i ).len   end, */
--                     a( i ).csid,  /* case when a( i ).csid  = -1 then null else a( i ).csid  end, */
--                     a( i ).csfrm  /* case when a( i ).csfrm = -1 then null else a( i ).csfrm end  */
--                 );
-- 
--                 commit;
-- 
--                 exception
--                     when others then
--                         rollback;
-- 
--             end;
-- 
--         end loop;
-- 
--     end if;
-- 
--     exception
--         when others then
--             null;
-- 
-- end test_;
-- /
-- 
-- show errors
-- 
-- /* *** *** */

--
create or replace function desc_( stm in varchar2, atr out attributes ) return number as
language java
name 'oracle.mti.hive.SqlDesc( java.lang.String, oracle.sql.ARRAY[] ) return java.math.BigDecimal';
/

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

    -- mixed PL/SQL and Java
    static function ODCITableDescribe( typ out anytype,
                                       stm in varchar2 ) return number,

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

--
create or replace type body hive_t as

    static function ODCITableDescribe( typ out anytype,
                                       stm in  varchar2 ) return number is

        --
        col anytype; 

        --
        ret number := odciconst.error;
        att attributes := attributes();

    begin 

        --
        ret := desc_( stm, att );

--        begin   /* *** DEBUG ONLY *** */
--
--            --
--            test_( null );  -- null purges old data
--            test_( att );
--
--            -- -- manually add attributes
--            -- att.extend;
--            -- att( att.count ) := attribute( 'cust_id',    2, 0,    -1, -1, -1, -1 );
--
--            -- att.extend;
--            -- att( att.count ) := attribute( 'last_name',  9, 4000, -1, -1, -1, -1 );
--
--            -- att.extend;
--            -- att( att.count ) := attribute( 'first_name', 9, 4000, -1, -1, -1, -1 );
--
--            -- ret := odciconst.success;
--
--        end;    /* *** DEBUG ONLY *** */
        

        --
        if ( ret = odciconst.success ) then

            if ( att.count > 0 ) then

                anytype.begincreate( dbms_types.typecode_object, col );

                --
                for i in 1 .. att.count loop

                    begin

                    --
                    col.addattr( att( i ).name,
                                 case when att( i ).code  = -1 then null else att( i ).code  end,
                                 case when att( i ).prec  = -1 then null else att( i ).prec  end,
                                 case when att( i ).scale = -1 then null else att( i ).scale end,
                                 case when att( i ).len   = -1 then null else att( i ).len   end,
                                 case when att( i ).csid  = -1 then null else att( i ).csid  end,
                                 case when att( i ).csfrm = -1 then null else att( i ).csfrm end );

                    exception
                        when others then
                            raise_application_error( -20002, 'WTF!' );

                    end;

                end loop;

                --
                col.endcreate;

                --
                anytype.begincreate( dbms_types.typecode_table, typ );
                typ.setinfo( null, null, null, null, null, col, dbms_types.typecode_object, 0 );
                typ.endcreate();

            else

                ret := odciconst.error;

            end if;

        end if;

        return ret;

    end;

end;
/
show errors

--
-- ... done!
--
