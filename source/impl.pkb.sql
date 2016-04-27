--------------------------------------------------------------------------------
--
-- 2016-04-19, NV - impl.pkb.sql
--

--
prompt ... running impl.pkb.sql

--
alter session set current_schema = hive;

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
    procedure begin_create( rec in out anytype ) is
    begin

        anytype.begincreate( dbms_types.typecode_object, rec );

    end begin_create;

    --
    procedure add_attribute( name  in     varchar2,
                             type  in     number,
                             prec  in     number,
                             scale in     number,
                             len   in     number,
                             chrid in     number,
                             chrfm in     number,
                             rec   in out anytype ) is
    begin

        rec.addattr( name, type, prec, scale, len, chrid, chrfm ); 

    end add_attribute;

    --
    procedure row_instance( rec in out anydataset ) is
    begin

        rec.AddInstance();

    end row_instance;

    --
    procedure row_piecewise( rec in out anydataset ) is
    begin

        rec.PieceWise();

    end row_piecewise;

    --
    procedure end_create( rec in out anytype ) is
    begin

        rec.endcreate();

    end end_create;

    --
    procedure swap_anytype( rec1 in     anytype,
                            rec2 in out anytype ) is
    begin

        anytype.begincreate( dbms_types.typecode_table, rec2 );
        rec2.setinfo( null, null, null, null, null, rec1, dbms_types.typecode_object, 0 );
        rec2.endcreate();

    end swap_anytype;

end impl;
/

show errors

--
-- ... done!
--
