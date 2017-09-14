--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - hive.typ.sql
--

/*
  Hive-ODCI - Copyright (C) 2006-2016 Metasystems Technologies Inc. (MTI)
  Nicholas Van Wyen
  
  This library is free software; you can redistribute it and/or modify it 
  under the terms of the GNU Lesser General Public License as published by 
  the Free Software Foundation; either version 2.1 of the License, or (at 
  your option) any later version.
  
  This library is distributed in the hope that it will be useful, but 
  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
  or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
  License for more details.
  
  You should have received a copy of the GNU Lesser General Public License 
  along with this library; if not, write to the
  
                  Free Software Foundation, Inc.
                  59 Temple Place, Suite 330,
                  Boston, MA 02111-1307 USA
*/

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
                                       bnd in  binds      default null,
                                       con in  connection default null ) return number,

    --
    static function ODCITablePrepare( ctx out hive_t,
                                      inf in  sys.ODCITabFuncInfo,
                                      stm in  varchar2,
                                      bnd in  binds      default null,
                                      con in  connection default null ) return number,

    --
    static function ODCITableStart( ctx in out hive_t,
                                    stm in     varchar2,
                                    bnd in     binds      default null,
                                    con in     connection default null ) return number,

    --
    member function ODCITableFetch( self in out hive_t,
                                    num  in     number,
                                    rws  out    anydataset ) return number,

    --
    member function ODCITableClose( self in hive_t ) return number,

    --
    static function do( stm in varchar2,
                        bnd in binds      default null,
                        con in connection default null ) return anydataset pipelined using hive_t
);
/

--
show errors

--
create or replace type body hive_t as

    --
    static function ODCITableDescribe( typ out anytype,
                                       stm in  varchar2,
                                       bnd in  binds      default null,
                                       con in  connection default null ) return number is

        --
        procedure trc_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_trace( :p0 ); end;'
              using 'hive_t::ODCITableDescribe: ' || txt;
            exception when others then null;

        end trc_;

        --
        procedure err_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_error( :p0 ); end;'
              using 'hive_t::ODCITableDescribe: ' || txt;
            exception when others then null;

        end err_;

    begin 

        --
        trc_( 'hive_t::ODCITableDescribe - called: '                            || chr( 10 ) || 
            '... stm: ' || case when ( stm is null ) then '{null}' else stm end || chr( 10 ) ||
            '... bnd: ' || binding.to_string( bnd )                             || chr( 10 ) ||
            '... con: ' || case when ( con is null ) then '{null}' else '{not null}' end );

        return impl.sql_describe( typ, stm, bnd, con );

        exception
            when others then
                err_( 'hive_t::ODCITableDescribe [' || stm || '] error: ' || sqlerrm );
                raise;

    end;

    --
    static function ODCITablePrepare( ctx out hive_t,
                                      inf in  sys.ODCITabFuncInfo,
                                      stm in  varchar2,
                                      bnd in  binds      default null,
                                      con in  connection default null ) return number is

        key     number;
        typ     anytype;

        prec    number; 
        scale   number; 
        len     number; 
        csid    number; 
        csfrm   number; 
        name    varchar2( 30 ); 

        ret     number; 

        --
        procedure trc_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_trace( :p0 ); end;'
              using 'hive_t::ODCITablePrepare: ' || txt;
            exception when others then null;

        end trc_;

        --
        procedure err_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_error( :p0 ); end;'
              using 'hive_t::ODCITablePrepare: ' || txt;
            exception when others then null;

        end err_;

    begin

        --
        trc_( 'hive_t::ODCITablePrepare - called: '                                      || chr( 10 ) || 
            '... inf: ' || case when ( inf is null ) then '{null}' else '{not null}' end || chr( 10 ) ||
            '... stm: ' || case when ( stm is null ) then '{null}' else stm end          || chr( 10 ) ||
            '... bnd: ' || binding.to_string( bnd )                                      || chr( 10 ) ||
            '... con: ' || case when ( con is null ) then '{null}' else '{not null}' end );

        ret := impl.sql_open( key, stm, bnd, con );

        if ( ret = odciconst.success ) then

            ret := inf.rettype.getattreleminfo( 1, prec, scale, len, csid, csfrm, typ, name ); 
            ctx := hive_t( key, typ );

            trc_( 'hive_t::ODCITablePrepare key: ' || to_char( key ) );

        else

            err_( 'hive_t::ODCITablePrepare error: ' || stm );
            return odciconst.error;

        end if;

        trc_( 'hive_t::ODCITablePrepare succeeded: ' || stm );
        return odciconst.success; 

        exception
            when others then
                err_( 'hive_t::ODCITablePrepare [' || to_char( key ) || '] error: ' || sqlerrm );
                raise;

    end;

    --
    static function ODCITableStart( ctx in out hive_t,
                                    stm in     varchar2,
                                    bnd in     binds      default null,
                                    con in     connection default null ) return number is

        ret number := odciconst.success;

        key integer;
        ref anytype;

        --
        procedure trc_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_trace( :p0 ); end;'
              using 'hive_t::ODCITableStart: ' || txt;
            exception when others then null;

        end trc_;

        --
        procedure err_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_error( :p0 ); end;'
              using 'hive_t::ODCITableStart: ' || txt;
            exception when others then null;

        end err_;

    begin

        --
        trc_( 'hive_t::ODCITableStart - called: '                                        || chr( 10 ) || 
            '... ctx: ' || case when ( ctx is null ) then '{null}' else '{not null}' end || chr( 10 ) ||
            '... stm: ' || case when ( stm is null ) then '{null}' else stm end          || chr( 10 ) ||
            '... bnd: ' || binding.to_string( bnd )                                      || chr( 10 ) ||
            '... con: ' || case when ( con is null ) then '{null}' else '{not null}' end );

        --
        ret := impl.sql_open( key, stm, bnd, con );

        trc_( 'hive_t::ODCITableStart [' || to_char( key ) || '] identified' );

        --
        if ( ret = odciconst.success ) then

            -- describe statment
            ret := impl.sql_describe( ref, stm, bnd, con );

        end if;

        --
        trc_( 'hive_t::ODCITableStart [' || stm || ']: returned: ' || to_char( ret ) );
        return ret;

        exception
            when others then
                err_( 'hive_t::ODCITableStart [' || stm || '] error: ' || sqlerrm );
                raise;

    end;

    --
    member function ODCITableFetch( self in out hive_t,
                                    num  in     number,
                                    rws  out    anydataset ) return number is

        ret number  := odciconst.error;
        rec records := records();

        --
        procedure trc_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_trace( :p0 ); end;'
              using 'hive_t::ODCITableFetch: ' || txt;
            exception when others then null;

        end trc_;

        --
        procedure err_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_error( :p0 ); end;'
              using 'hive_t::ODCITableFetch: ' || txt;
            exception when others then null;

        end err_;

        --
        procedure wrn_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_warn( :p0 ); end;'
              using 'hive_t::ODCITableFetch: ' || txt;
            exception when others then null;

        end wrn_;

    begin

        --
        trc_( 'hive_t::ODCITableFetch - called: '                                           || chr( 10 ) || 
            '... self: ' || case when ( self is null ) then '{null}' else '{not null}' end  || chr( 10 ) ||
            '... num:  ' || case when ( num is null ) then '{null}' else to_char( num ) end || chr( 10 ) ||
            '... rws:  ' || '<out>' );


        -- retrieve the next "num" records
        ret := impl.sql_fetch( self.key, num, rec );

        trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] retrieve next: ' || to_char( num ) );

        --
        if ( ret = odciconst.success ) then

            --
            if ( rec is not null ) then

                --
                if ( rec.count > 0 ) then

                    anydataset.begincreate( dbms_types.typecode_object, self.ref, rws );
                    rws.addinstance();
                    rws.piecewise();

                    trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] began piecewise recordset creation' );

                    --
                    for i in 1 .. rec.count loop

                        if ( rec( i ).code = dbms_types.typecode_varchar2 ) then

                            trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] set typecode_varchar2' );
                            rws.setvarchar2( rec( i ).val_varchar2 );

                        elsif ( rec( i ).code = dbms_types.typecode_number ) then

                            trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] set typecode_number' );
                            rws.setnumber( rec( i ).val_number );

                        elsif ( rec( i ).code = dbms_types.typecode_date ) then

                            trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] set typecode_date' );
                            rws.setdate( rec( i ).val_date );

                        elsif ( rec( i ).code = dbms_types.typecode_timestamp ) then

                            trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] set typecode_timestamp' );
                            rws.settimestamp( rec( i ).val_timestamp );

                        elsif ( rec( i ).code = dbms_types.typecode_clob ) then

                            trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] set typecode_clob' );
                            rws.setclob( rec( i ).val_clob );

                        elsif ( rec( i ).code = dbms_types.typecode_blob ) then

                            trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] set typecode_blob' );
                            rws.setblob( rec( i ).val_blob );

                        else

                            err_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] record type code ['
                                                             || to_char( rec( i ).code )
                                                             ||'] not supported for column index ['
                                                             || to_char( i ) || ']' );

                            raise_application_error( -20210, 'Record type code ['
                                                           || to_char( rec( i ).code )
                                                           ||'] not supported for column index ['
                                                           || to_char( i ) || ']' );

                        end if;

                    end loop;

                    --
                    rws.endcreate();

                    trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] eneded recordset creation' );

                else

                    wrn_( to_char( self.key ) || ' -- Record count is zero' );
                    ret := odciconst.error;

                end if;

            else

                trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] Record set is null' );

            end if;

        end if;

        trc_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] returned: ' || to_char( ret ) );
        return ret;

        exception
            when others then
                err_( 'hive_t::ODCITableFetch [' || to_char( self.key ) || '] error: ' || sqlerrm );
                raise;

    end;

    --
    member function ODCITableClose( self in hive_t ) return number is

        --
        procedure trc_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_trace( :p0 ); end;'
              using 'hive_t::ODCITableClose: ' || txt;
            exception when others then null;

        end trc_;

        --
        procedure err_( txt in varchar2 ) is
        begin

            execute immediate 'begin impl.log_error( :p0 ); end;'
              using 'hive_t::ODCITableClose: ' || txt;
            exception when others then null;

        end err_;

    begin

        trc_( 'hive_t::ODCITableClose [' || to_char( self.key ) || '] closed' );
        return impl.sql_close( self.key );

        exception
            when others then
                err_( 'hive_t::ODCITableClose [' || to_char( self.key ) || '] error: ' || sqlerrm );
                raise;

    end;

end;
/

--
show errors

--
-- ... done!
--
