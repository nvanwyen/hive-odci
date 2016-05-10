--------------------------------------------------------------------------------
--
-- 2016-04-19, NV - impl.pkb.sql
--

--
prompt ... running impl.pkb.sql

--
GGalter session set current_schema = hive;

--
create or replace package body impl as

    --
    connect_ session;

    --
    function param_( n in varchar2 ) return varchar2 is

        v varchar2( 4000 );

    begin

        --
        select a.value into v
          from param$ a
         where a.name = n;

        --
        return v;

        --
        exception
            when no_data_found then
                return null;

    end param_;

    --
    function connection_ return session is
    begin

        return connect_;

    end connection_;

    --
    procedure connection_( con in session ) is
    begin

        --
        connect_.host := case when ( con.host is null )
                              then case when ( connect_.host is null )
                                        then param_( 'default_hive_host' )
                                        else connect_.host
                                   end
                              else con.host
                         end;

        --
        connect_.port := case when ( con.port is null )
                              then case when ( connect_.port is null )
                                        then param_( 'default_hive_port' )
                                        else connect_.port
                                   end
                              else con.port
                         end;

        --
        connect_.user := case when ( con.user is null )
                              then case when ( connect_.user is null )
                                        then param_( 'default_hive_user' )
                                        else connect_.user
                                   end
                              else con.user
                         end;

        --
        connect_.pass := case when ( con.pass is null )
                              then case when ( connect_.pass is null )
                                        then param_( 'default_hive_pass' )
                                        else connect_.pass
                                   end
                              else con.pass
                         end;

    end connection_;

    -- 
    procedure connection( usr in varchar2,
                          pwd in varchar2 ) is

        con session := connect_;

    begin

        con.user := usr;
        con.pass := pwd;

        connection( con );

    end connection;

    -- 
    procedure connection( hst in varchar2,
                          prt in service,
                          usr in varchar2,
                          pwd in varchar2 ) is

        con session := connect_;

    begin

        --
        con.host := case when ( hst is null )
                         then param_( 'default_hive_host' )
                         else hst
                    end;

        --
        con.port := case when ( prt is null )
                         then param_( 'default_hive_port' )
                         else prt
                    end;

        --
        con.user := case when ( usr is null )
                         then param_( 'default_hive_user' )
                         else usr
                    end;

        --
        con.pass := case when ( pwd is null )
                         then param_( 'default_hive_pass' )
                         else pwd
                    end;

        --
        connection( con );

    end connection;

    --
    procedure connection( con in session ) is
    begin

        connection_( con );

    end connection;

    --
    function connection return session is
    begin

        return connection_;

    end connection;

    --
    procedure initialize( obj out nocopy anytype ) is

        typ anytype;

    begin

        --
        anytype.begincreate( dbms_types.typecode_object, typ );
        obj := typ;

    end initialize;

    --
    procedure finalize( obj in out nocopy anytype ) is
    begin

        obj.endcreate;

    end finalize;

    --
    procedure attribute( obj   in out nocopy anytype,
                         name  in            varchar2,
                         code  in            pls_integer,
                         prec  in            pls_integer,
                         scale in            pls_integer,
                         len   in            pls_integer,
                         csid  in            pls_integer,
                         csfrm in            pls_integer,
                         attr  in            anytype default null) is
    begin


        --
        obj.addattr( name, code, prec, scale, len, csid, csfrm, attr );

    end attribute;

    --
    procedure clone( trg in out nocopy anytype,
                     src in            anytype ) is
    begin

        initialize( trg );
        trg.setinfo( null, null, null, null, null, src, dbms_types.typecode_object, 0 );
        finalize( trg );

    end clone;

--    --
--    procedure output( obj in anytype ) is
--
--        typ anytype;
--        val varchar2( 32767 );
--        ret number;
--
--    BEGIN
--
--        case obj.gettype( typ )
--        when dbms_types.typecode_number then
--            val := 'number: ' || to_char( obj.accessnumber );
--        when dbms_types.typecode_varchar2 then
--            val := 'varchar2: ' || obj.accessvarchar2;
--        when dbms_types.typecode_char then
--            val := 'char: ' || rtrim( obj.accesschar );
--        when dbms_types.typecode_date then
--            val := 'date: ' || to_char( obj.accessdate, 'yyyy-mm-dd hh24:mi:ss' );
--        when dbms_types.typecode_object then
--            execute immediate 'declare ' ||
--                              '    o ' || obj.gettypename || '; ' ||
--                              '    a anydata := :ad; ' ||
--                              'begin ' ||
--                              '    :res := a.getobject( o ); ' ||
--                              '    :ret := o.print(); ' ||
--                              'end;'
--                              using in obj, out ret, out val;
--            val := obj.GetTypeName || ': ' || val;
-- 	    when dbms_types.typecode_ref then
--            execute immediate 'declare ' ||
--                              '    r ' || obj.gettypename || '; ' ||
--                              '    o ' || substr( obj.gettypename,
--                                                  instr( obj.gettypename, ' ' ) ) || '; ' ||
--                              '    a anydata := :ad; ' ||
--                              'begin ' ||
--                              '    :res := a.getref( r ); ' ||
--                              '    utl_ref.select_object( r, o );' ||
--                              '    :ret := o.print(); ' ||
--                              'end;'
--                              using in obj, out ret, out val;
--            val := obj.GetTypeName || ': ' || val;
--        else
--            val := '<data of type ' || obj.gettypename ||'>';
--        end case;
--
--    dbms_output.put_line( val );
--
--    exception
--        when others
--            then
--                if instr( sqlerrm, 'component ''print'' must be declared' ) > 0 then
--                    dbms_output.put_line( obj.gettypename || ': <no print() function>' );
--                else
--                    dbms_output.put_line( 'error: ' || sqlerrm );
--                end if;
--
--    end output;

end impl;
/

show errors

--
-- ... done!
--
