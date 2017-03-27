
alter session set current_schema = scott;

create type some_type as object
(  

  -- store the types of the returned rows:
  row_types        anytype,

  -- Only one row should be retured. I need a flag
  -- that tells me, if the row was already returned
  row_was_returned number,

  -- do: This function needs not be implemented because of the 'pipelined using' construct.
  -- Behind the scenes, ODCITableDescribe and so on are called through the call to table( some_type.do( ... ) )
  -- static function do( do_param in number ) return anydataset pipelined using some_type,

  static function ODCITableDescribe( record_table out anytype, do_param in number ) return number, 
  static function ODCITablePrepare( sctx  out some_type, tab_func_info in sys.ODCITabFuncInfo, do_param in number ) return number, 
  static function ODCITableStart( sctx  in out some_type, do_param in number ) return number, 
  member function ODCITableFetch( self  in out some_type, nrows in number, record_out out anydataset ) return number,
  member function ODCITableClose( self  in some_type  ) return number
 )
/

create or replace type body some_type as

  static function ODCITableDescribe( record_table out anytype, do_param in number ) return number is
    record_structure anytype;
  begin
    dbms_output.put_line( 'ODCITableDescribe, do_param: ' || do_param );

    -- The type that describes the structure of a record is created. This type
    -- must be of dbms_types.typecode_object.
    anytype.begincreate( dbms_types.typecode_object, record_structure );

    -- Adding some attributes to the type being created. These attributes specify the
    -- column names, their datatype and the precision
    --
    -- The first column ( named col1 ) will be a varchar2( 10 )
    record_structure.addattr( 
                aname     => 'col_1',
                typecode  =>  dbms_types.typecode_varchar2,
                prec      =>  null,
                scale     =>  null,
                len       =>  10,
                csid      =>  null,
                csfrm     =>  null
     );

    -- The second column will be a number( 6.2 ). Prec( ision ) and scale must
    -- be stated.
    record_structure.addattr( 
                aname     => 'col_2',
                typecode  =>  dbms_types.typecode_number,
                prec      =>  6,
                scale     =>  2,
                len       =>  null,
                csid      =>  null,
                csfrm     =>  null
     );

    -- The third column will be a date. Neither preciscion, scale nor len
    -- need be specified.
    record_structure.addattr( 
                aname     => 'col_3',
                typecode  =>  dbms_types.typecode_date,
                prec      =>  null,
                scale     =>  null,
                len       =>  null,
                csid      =>  null,
                csfrm     =>  null
     );

    -- I am done adding attributes:
    record_structure.endcreate;

    -- Of course, in this example, I didn't really create a dynamic return type
    -- since all three columns will always be the same. But I thought I just want to
    -- show the idea.

    -- Now, after creating the record structure, I also need a to create a nested table
    -- of that record structure type. This is indicated with dbms_types.typecode_table.
    anytype.begincreate( dbms_types.typecode_table, record_table );

    record_table.setinfo( null, null, null, null, null, record_structure, dbms_types.typecode_object, 0 );
    record_table.endcreate();

    return odciconst.success;

  exception when others then
    -- indicate that an error has occured somewhere.
    return odciconst.error;
  end;

  -- ODCITablePrepare creates an instance of some_type and returns it through the sctx out parameter.
  static function ODCITablePrepare( sctx out some_type, tab_func_info in sys.ODCITabFuncInfo, do_param in number ) return number is
    prec         pls_integer;
    scale        pls_integer;
    len          pls_integer;
    csid         pls_integer;
    csfrm        pls_integer;
    record_desc  anytype;
    aname        varchar2( 30 );
    dummy        pls_integer;
  begin
    dbms_output.put_line( 'ODCITablePrepare, do_param: ' || do_param );

    -- this is a bit mystic, imho: Through tab_func_info.RetType, it's possible to access the record_table that
    -- was created in ODCITableDescribe.
    --
    -- With GetAttrElemInfo, I can get the record_structure that was created in ODCITableDescribe. This record_structure
    -- is returned in the out parameter record_desc.
    -- The parameters prec, scale, len, csid, csfrm and aname are ignored.
    dummy := tab_func_info.RetType.GetAttrElemInfo( null, prec, scale, len, csid, csfrm, record_desc, aname );

    -- Now, I am ready to construct an instance of some_type.
    -- The first parameter will be stored in the member row_types, the second in row_was_returned.
    sctx := some_type( record_desc, 0 );
    return odciconst.success;
  end;


  static function ODCITableStart( sctx in out some_type, do_param in number  ) return number is
  begin

    dbms_output.put_line( 'ODCITableStart, do_param: ' || do_param );
    return odciconst.success;
  end;


  member function ODCITableFetch( self in out some_type, nrows in number, record_out out anydataset ) return number is
  begin
    dbms_output.put_line( 'ODCITableFetch, nrows:' || nrows );
    record_out := null;

    if row_was_returned = 1 then
      -- record_out being null indicates last record was already fetched
      return ODCIconst.success;
    end if;

    row_was_returned := 1;

    anydataset.begincreate( dbms_types.typecode_object, self.row_types, record_out );

    record_out.addinstance;
    record_out.piecewise();

    -- Setting the returned values:
    record_out.setvarchar2( 'foo' );
    record_out.setnumber  (   5.9 );
    record_out.setdate    ( sysdate-10 );

    record_out.endcreate;

    return odciconst.success;
  end;


  member function ODCITableClose( self in some_type ) return number is
  begin
    dbms_output.put_line( 'ODCITableClose' );
    return odciconst.success;
  end;

end;
/

create function some_recs( n in number ) return anydataset pipelined using some_type;
/

set serveroutput on
set arraysize    4

-- select * from table( some_type.do( 42 ) );
select * from table( some_recs( 42 ) );

drop function some_recs;
drop type some_type;

exit
