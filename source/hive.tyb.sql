--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - hive.tyb.sql
--

--
prompt ... running hive.tyb.sql

-- generic ANYTYPE abstraction for Hive queries

--
create or replace type body hive_t as

    --
    static function ODCITableDescribe( type  out anytype,
                                       stmt  in  varchar2 ) return number is
    begin

        --
        return ODCIConst.Success;

    end;

    --
    static function ODCITablePrepare( this  out hive_t,
                                      info  in  sys.odcitabfuncinfo,
                                      stmt  in  varchar2 ) return number is
    begin

        --
        return ODCIConst.Success;

    end;


    --
    static function ODCITableStart( this in out hive_t,
                                    stmt in     varchar2 ) return number is
    begin

        --
        return ODCIConst.Success;

    end;

    --
    member function ODCITableFetch( this  in out hive_t,
                                    rows  in     number,
                                    type  out    anydataset ) return number is
    begin

        --
        return ODCIConst.Success;

    end;

    --
    member function ODCITableClose( this in hive_t ) return number is
    begin

        --
        return ODCIConst.Success;

    end;

end;
/

--
show errors

--
-- ... done!
--
