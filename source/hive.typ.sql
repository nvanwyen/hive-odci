--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - hive.typ.sql
--

--
prompt ... running hive.typ.sql

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
                                       stm in varchar2 ) return number,

    --
    member function ODCITableFetch( self in out hive_t,
                                    num  in     number,
                                    rws  out    anydataset ) return number,

    --
    member function ODCITableClose( self in hive_t ) return number as
    language java
    name 'oracle.mti.hive.ODCITableClose() return java.math.BigDecimal'
);
/

--
create or replace type body hive_t as

    static function ODCITableDescribe( typ out anytype,
                                       stm in  varchar2 ) return number is

        --
        col anytype; 

        --
        ret number     := odciconst.error;
        att attributes := attributes();

    begin 

        --
        ret := impl.sql_describe( stm, att );

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

    --
    member function ODCITableFetch( self in out hive_t,
                                    num  in     number,
                                    rws  out    anydataset ) return number is

        col anytype; 

        --
        ret number  := odciconst.error;
        rec records := records();

    begin

        --
        ret := impl.sql_fetch( num, rec );

        --
        if ( ret = odciconst.success ) then

            if ( rec.count > 0 ) then

                anydataset.begincreate( dbms_types.typecode_object, col, rws );
                rws.addinstance();
                rws.piecewise();

                --
                for i in 1 .. rec.count loop

                    --
                    case ( rec( i ).code )

                        when dbms_types.typecode_varchar2 then
                            rws.setvarchar2( rec( i ).val_varchar2 );

                        when dbms_types.typecode_number then
                            rws.setnumber( rec( i ).val_number );

                        when dbms_types.typecode_date then
                            rws.setdate( rec( i ).val_date );

                        when dbms_types.typecode_clob then
                            rws.setclob( rec( i ).val_clob );

                        when dbms_types.typecode_blob then
                            rws.setblob( rec( i ).val_blob );

                    end case;

                end loop;

                --
                rws.endcreate();

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
