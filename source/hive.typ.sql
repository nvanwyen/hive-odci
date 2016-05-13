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
                                       stm in  varchar2 ) return number,

    --
    static function ODCITableStart( ctx in out hive_t,
                                    stm in     varchar2 ) return number,

    -- --
    -- static function ODCITableStart( ctx in out hive_t,
    --                                 stm in     varchar2 ) return number as
    -- language java
    -- name 'oracle.mti.hive.SqlOpen( oracle.sql.STRUCT[], java.lang.String ) return java.math.BigDecimal',

    --
    member function ODCITableFetch( self in out hive_t,
                                    num  in     number,
                                    rws  out    anydataset ) return number,

    --
    member function ODCITableClose( self in hive_t ) return number
);
/

--
create or replace type body hive_t as

    --
    static function ODCITableDescribe( typ out anytype,
                                       stm in  varchar2 ) return number is
    begin 

        --
        return impl.sql_describe( stm, typ );

    end;

    --
    static function ODCITableStart( ctx in out hive_t,
                                    stm in     varchar2 ) return number is

        ret number := odciconst.error;

    begin

        --
        ctx := hive_t( null, null );

        --
        ret := impl.sql_open( stm, ctx.key );

        --
        if ( ret = odciconst.success ) then

            -- describe statment
            ret := impl.sql_describe( stm, ctx.ref );

            --
            debug_info( 'ODCITableStart ref', ctx.ref ); /* *** debug *** */
            --

        end if;

        return ret;

    end;

    --
    member function ODCITableFetch( self in out hive_t,
                                    num  in     number,
                                    rws  out    anydataset ) return number is

        ret number  := odciconst.error;
        rec records := records();

    begin

        --
        debug_info( 'ODCITableFetch self.ref', self.ref ); /* *** debug *** */
        --

        -- retrieve the next "num" records
        ret := impl.sql_fetch( self.key, num, rec );

        --
        debug_records( 'ODCITableFetch', self.key, num, rec ); /* *** debug *** */
        --

        --
        if ( ret = odciconst.success ) then

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
