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
    ref anytype,


    --
    static function ODCITableDescribe( typ out anytype,
                                       stm in  varchar2,
                                       bnd in  binds      := null,
                                       con in  connection := null ) return number,

    --
    static function ODCITablePrepare( ctx out hive_t,
                                      inf in  sys.ODCITabFuncInfo,
                                      stm in  varchar2,
                                      bnd in  binds      := null,
                                      con in  connection := null ) return number,

    --
    static function ODCITableStart( ctx in out hive_t,
                                    stm in     varchar2,
                                    bnd in     binds      := null,
                                    con in     connection := null ) return number,

    --
    member function ODCITableFetch( self in out hive_t,
                                    num  in     number,
                                    rws  out    anydataset ) return number,

    --
    member function ODCITableClose( self in hive_t ) return number
);
/

show errors

--
create or replace type body hive_t as

    --
    static function ODCITableDescribe( typ out anytype,
                                       stm in  varchar2,
                                       bnd in  binds      := null,
                                       con in  connection := null ) return number is
    begin 

        --
        return impl.sql_describe( typ, stm, bnd, con );

    end;

    --
    static function ODCITablePrepare( ctx out hive_t,
                                      inf in  sys.ODCITabFuncInfo,
                                      stm in  varchar2,
                                      bnd in  binds      := null,
                                      con in  connection := null ) return number is

        key     number;
        typ     anytype;

        prec    number; 
        scale   number; 
        len     number; 
        csid    number; 
        csfrm   number; 
        name    varchar2( 30 ); 

        ret     number; 

    begin

        ret := impl.sql_open( key, stm, bnd, con );

        if ( ret = odciconst.success ) then

            ret := inf.rettype.getattreleminfo( 1, prec, scale, len, csid, csfrm, typ, name ); 
            ctx := hive_t( key, typ );

        else

            return odciconst.error;

        end if;

        return odciconst.success; 

    end;

    --
    static function ODCITableStart( ctx in out hive_t,
                                    stm in     varchar2,
                                    bnd in     binds      := null,
                                    con in     connection := null ) return number is

        ret number := odciconst.success;

        key integer;
        ref anytype;

    begin

        --
        ret := impl.sql_open( key, stm, bnd, con );

        --
        if ( ret = odciconst.success ) then

            -- describe statment
            ret := impl.sql_describe( ref, stm, bnd, con );

        end if;

        --
        return ret;

    end;

    --
    member function ODCITableFetch( self in out hive_t,
                                    num  in     number,
                                    rws  out    anydataset ) return number is

        ret number  := odciconst.error;
        rec records := records();

    begin

        -- retrieve the next "num" records
        ret := impl.sql_fetch( self.key, num, rec );

        --
        if ( ret = odciconst.success ) then

            --
            if ( rec is not null ) then

                --
                if ( rec.count > 0 ) then

                    anydataset.begincreate( dbms_types.typecode_object, self.ref, rws );
                    rws.addinstance();
                    rws.piecewise();

                    --
                    for i in 1 .. rec.count loop

                        if ( rec( i ).code = dbms_types.typecode_varchar2 ) then

                            rws.setvarchar2( rec( i ).val_varchar2 );

                        elsif ( rec( i ).code = dbms_types.typecode_number ) then

                            rws.setnumber( rec( i ).val_number );

                        elsif ( rec( i ).code = dbms_types.typecode_date ) then

                            rws.setdate( rec( i ).val_date );

                        elsif ( rec( i ).code = dbms_types.typecode_timestamp ) then

                            rws.settimestamp( rec( i ).val_timestamp );

                        elsif ( rec( i ).code = dbms_types.typecode_clob ) then

                            rws.setclob( rec( i ).val_clob );

                        elsif ( rec( i ).code = dbms_types.typecode_blob ) then

                            rws.setblob( rec( i ).val_blob );

                        else

                            raise_application_error( -20010, 'Record type code ['
                                                           || to_char( rec( i ).code )
                                                           ||' ] not supported for column index ['
                                                           || to_char( i ) || ']' );

                        end if;

                    end loop;

                    --
                    rws.endcreate();

                else

                    ret := odciconst.error;

                end if;

            end if;

        end if;

        return ret;

    end;

    --
    member function ODCITableClose( self in hive_t ) return number is
    begin

        return impl.sql_close( self.key );

    end;

end;
/
show errors

--
-- ... done!
--
