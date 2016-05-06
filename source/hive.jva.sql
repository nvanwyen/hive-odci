--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - hive.jva.sql
--

set linesize 160

--
prompt ... running hive.jva.sql

--
alter session set current_schema = hive;

--
create or replace and compile java source named "hive" as

package oracle.mti;

//
import java.io.*;
import java.sql.*;
import java.math.*;
import java.util.*;
import java.util.regex.*;

import oracle.sql.*;
import oracle.jdbc.*;
import oracle.ODCI.*;
import oracle.CartridgeServices.*;

//
public class DBMS_TYPES
{
    /*
        This java class duplicates the PL/SQL SYS.DBMS_TYPES
        package specification type codes for convenience
        as of 12c (may not be applicable in later version)
    */
    public static final int TYPECODE_DATE            =  12;
    public static final int TYPECODE_NUMBER          =   2;
    public static final int TYPECODE_RAW             =  95;
    public static final int TYPECODE_CHAR            =  96;
    public static final int TYPECODE_VARCHAR2        =   9;
    public static final int TYPECODE_VARCHAR         =   1;
    public static final int TYPECODE_MLSLABEL        = 105;
    public static final int TYPECODE_BLOB            = 113;
    public static final int TYPECODE_BFILE           = 114;
    public static final int TYPECODE_CLOB            = 112;
    public static final int TYPECODE_CFILE           = 115;
    public static final int TYPECODE_TIMESTAMP       = 187;
    public static final int TYPECODE_TIMESTAMP_TZ    = 188;
    public static final int TYPECODE_TIMESTAMP_LTZ   = 232;
    public static final int TYPECODE_INTERVAL_YM     = 189;
    public static final int TYPECODE_INTERVAL_DS     = 190;

    public static final int TYPECODE_REF             = 110;
    public static final int TYPECODE_OBJECT          = 108;
    public static final int TYPECODE_VARRAY          = 247;            /* COLLECTION TYPE */
    public static final int TYPECODE_TABLE           = 248;            /* COLLECTION TYPE */
    public static final int TYPECODE_NAMEDCOLLECTION = 122;
    public static final int TYPECODE_OPAQUE          = 58;             /* OPAQUE TYPE */

    /* 
        These typecodes are for use in AnyData api only and are short forms
        for the corresponding char typecodes with a charset form of SQLCS_NCHAR.
    */
    public static final int TYPECODE_NCHAR           = 286;
    public static final int TYPECODE_NVARCHAR2       = 287;
    public static final int TYPECODE_NCLOB           = 288;

    /* Typecodes for Binary Float, Binary Double and Urowid. */
    public static final int TYPECODE_BFLOAT          = 100;
    public static final int TYPECODE_BDOUBLE         = 101;
    public static final int TYPECODE_UROWID          = 104;

    public static final int SUCCESS                  = 0;
    public static final int NO_DATA                  = 100;
};

//
public class hive_exception extends Exception
{
    public hive_exception( String msg )
    {
        super( msg );
    }
};

//
public class hive_parameter
{
    //
    static public String value( String name )
    {
        //
        String val = null;

        //
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            String sql = "select value " +
                           "from param$ " +
                          "where name = ?";

            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            stm = con.prepareStatement( sql );
            stm.setString( 1, name );

            //
            ResultSet rst = stm.executeQuery();

            if ( rst.next() )
                val = rst.getString( "value" );

            //
            rst.close();
            stm.close();
        }
        catch ( SQLException ex )
        {
            //
        }
        catch ( Exception ex )
        {
            //
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();
            }
            catch ( SQLException ex ) {}
            catch ( Exception ex ) {}

            // *** do not close the "default" connection ***
        }

        //
        return val;
    }
};

// stored context records
public class hive_connection
{
    //
    private static hive_parameter param_;
    // private static String driver_ = param_.value( "hive_jdbc_driver" );
    // String url_ = param_.value( "hive_jdbc_url" );

    private static String driver_ = "com.ddtek.jdbc.hive.HiveDriver";
    String url_ = "jdbc:datadirect:hive://%host%:%port%;User=%user%;Password=%pass%";

    //
    private String host_;
    private String port_;
    private String user_;
    private String pass_;

    //
    Connection conn_;

    //
    public hive_connection()
    {
        host_ = "";
        port_ = "";
        user_ = "";
        pass_ = "";
    }

    //
    public hive_connection( String host, String port )
    {
        host_ = host;
        port_ = port;
    }

    //
    public hive_connection( String host, String port, String user, String pass )
    {
        host_ = host;
        port_ = port;
        user_ = user;
        pass_ = pass;
    }

    //
    static public void loadDriver() throws hive_exception
    {
        try
        {
            Class.forName( driver_ );
        }
        catch ( ClassNotFoundException e )
        {
            throw new hive_exception( "Driver class not foound: " + driver_ );
        }
    }

    //
    static public String getDriverName()
    {
        return driver_;
    }

    //
    public void setHost( String val ) { host_ = val; }
    public void setPort( String val ) { port_ = val; }
    public void setUser( String val ) { user_ = val; }
    public void setPass( String val ) { pass_ = val; }

    //
    public String getHost() { return host_; }
    public String getPort() { return port_; }
    public String getUser() { return user_; }
    public String getPass() { return pass_; }

    //
    public String getUrl() throws hive_exception
    {
        if ( ( host_.length() > 0 )
          && ( port_.length() > 0 )
          && ( user_.length() > 0 )
          && ( pass_.length() > 0 ) )
        {
            if ( url_.indexOf( '%' ) >= 0 )
            {
                url_.replace( "%h%", host_ );
                url_.replace( "%p%", port_ );
                url_.replace( "%u%", user_ );
                url_.replace( "%w%", pass_ );
            }
        }
        else
        {
            if ( host_.length() == 0 )
                throw new hive_exception( "Missing host in connection data" );

            if ( port_.length() == 0 )
                throw new hive_exception( "Missing port in connection data" );

            if ( user_.length() == 0 )
                throw new hive_exception( "Missing user in connection data" );

            if ( pass_.length() == 0 )
                throw new hive_exception( "Missing password in connection data" );
        }

        // debug only
        url_ = "jdbc:datadirect:hive://orabdc.local:10000;User=oracle;Password=welcome1";
        System.out.println( url_ );
        return url_;
    }

    //
    public boolean loadConnection()
    {
        if ( host_.length() == 0 )
            host_ = "orabdc.local"; // param_.value( "default_hive_host" );

        if ( port_.length() == 0 )
            port_ = "10000"; // param_.value( "default_hive_port" );

        if ( user_.length() == 0 )
            user_ = "oracle"; // param_.value( "default_hive_user" );

        if ( pass_.length() == 0 )
            pass_ = "welcome1"; // param_.value( "default_hive_pass" );

        return ( ( host_.length() > 0 )
              && ( port_.length() > 0 )
              && ( user_.length() > 0 )
              && ( pass_.length() > 0 ) );
    }

    //
    public Connection getConnection()
    {
        return conn_;
    }

    //
    public Connection createConnection() throws SQLException, hive_exception
    {
        if ( getConnection() == null )
        {
            //
            if ( loadConnection() )
            {
                String url = getUrl();

                if ( url.length() > 0 )
                    conn_ = DriverManager.getConnection( url );
            }
            else
                throw new hive_exception( "Could not load connection data" );
        }

        return conn_;
    }
};

//
public class hive_context
{
    // connectivity
    //
    private hive_connection   hcn_;

    // locals
    private String            sql_;
    private PreparedStatement stm_;
    private ResultSet         rst_;
    private ResultSetMetaData rmd_;

    // ctor
    //
    public hive_context( String sql ) throws SQLException, hive_exception
    {
        sql_ = sql;

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( hcn_ == null )
            hcn_ = new hive_connection();

        hcn_.loadDriver();
        hcn_.createConnection();
    }

    //
    public boolean ready()
    {
        return ( ! ( rst_ == null ) );
    }

    //
    public void clear()
    {
        hcn_ = null;
        sql_ = null;
        stm_ = null;
        rst_ = null;
        rmd_ = null;
    }

    // members
    //
    public String getSql()
    {
        return sql_;
    }

    //
    public PreparedStatement getPreparedStatement() throws SQLException, hive_exception
    {
        setPreparedStatement(); return stm_;
    }

    //
    public ResultSet getResultSet() throws SQLException, hive_exception
    {
        setResultSet(); return rst_;
    }

    //
    public ResultSetMetaData getResultSetMetaData() throws SQLException, hive_exception
    {
        setResultSetMetaData(); return rmd_;
    }

    // metadata
    //
    public int columnCount() throws SQLException
    {
        return rmd_.getColumnCount();
    }

    //
    public int columnType( int i ) throws SQLException
    {
        return rmd_.getColumnType( i );
    }

    // recordset
    //
    public boolean next() throws SQLException
    {
        return rst_.next();
    }

    //
    public int rowNumber() throws SQLException
    {
        return ( ready() ) ? rst_.getRow() : -1;
    }

    // data
    //
    public Object getObject( int i ) throws SQLException
    {
        return rst_.getObject( i );
    }

    //
    public Object getObject( String c ) throws SQLException
    {
        return rst_.getObject( c );
    }

    //
    public BigDecimal getBigDecimal( int i ) throws SQLException
    {
        return rst_.getBigDecimal( i );
    }

    //
    public BigDecimal getBigDecimal( String c ) throws SQLException
    {
        return rst_.getBigDecimal( c );
    }

    //
    public boolean getBoolean( int i ) throws SQLException
    {
        return rst_.getBoolean( i );
    }

    //
    public boolean getBoolean( String c ) throws SQLException
    {
        return rst_.getBoolean( c );
    }

    //
    public int getInt( int i ) throws SQLException
    {
        return rst_.getInt( i );
    }

    //
    public int getInt( String c ) throws SQLException
    {
        return rst_.getInt( c );
    }

    //
    public long getLong( int i ) throws SQLException
    {
        return rst_.getLong( i );
    }

    //
    public long getLong( String c ) throws SQLException
    {
        return rst_.getLong( c );
    }

    //
    public float getFloat( int i ) throws SQLException
    {
        return rst_.getFloat( i );
    }

    //
    public float getFloat( String c ) throws SQLException
    {
        return rst_.getFloat( c );
    }

    //
    public Date getDate( int i ) throws SQLException
    {
        return rst_.getDate( i );
    }

    //
    public Date getDate( String c ) throws SQLException
    {
        return rst_.getDate( c );
    }

    //
    public String getString( int i ) throws SQLException
    {
        return rst_.getString( i );
    }

    //
    public String getString( String c ) throws SQLException
    {
        return rst_.getString( c );
    }

    //
    public Timestamp getTimestamp( int i ) throws SQLException
    {
        return rst_.getTimestamp( i );
    }

    //
    public Timestamp getTimestamp( String c ) throws SQLException
    {
        return rst_.getTimestamp( c );
    }

    //
    public boolean execute() throws SQLException, hive_exception
    {
        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        return setResultSet();
    }

    //
    public ResultSetMetaData descSql() throws SQLException, hive_exception
    {
        ResultSetMetaData rmd = null;

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( rst_ == null )
        {
            PreparedStatement stm = hcn_.getConnection().prepareStatement( limitSql( sql_ ) );
            ResultSet rst = stm.executeQuery();
            rmd = rst.getMetaData();
        }
        else
        {
            if ( setResultSetMetaData() )
                rmd = getResultSetMetaData();
        }

        return rmd;
    }

    // private functions ...

    //
    private boolean setPreparedStatement() throws SQLException, hive_exception
    {
        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( stm_ == null )
            stm_ = hcn_.getConnection().prepareStatement( sql_ );

        return ( ! ( stm_ == null ) );
    }

    //
    private boolean setResultSet() throws SQLException, hive_exception
    {
        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( rst_ == null )
        {
            if ( setPreparedStatement() )
                rst_ = stm_.executeQuery();
        }

        return ( ! ( rst_ == null ) );
    }

    //
    private boolean setResultSetMetaData() throws SQLException, hive_exception
    {
        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( rmd_ == null )
        {
            if ( setResultSet() )
                rmd_ = rst_.getMetaData();
        }

        return ( ! ( rmd_ == null ) );
    }

    //
    private static String limitSql( String sql )
    {
        String qry = sql.trim();
        
        if ( qry.toLowerCase().indexOf( "select" ) == 0 )
        {
            Pattern ptn = Pattern.compile( "limit [0-9]" );

            while ( qry.indexOf( "  " ) >= 0 )
                qry = qry.replace( "  ", " " );

            while ( qry.indexOf( "\n" ) >= 0 )
                qry = qry.replace( "\n", " " );

            while ( qry.indexOf( "\r" ) >= 0 )
                qry = qry.replace( "\r", " " );

            Matcher reg = ptn.matcher( qry.toLowerCase() );

            if ( reg.find() )
                qry = qry.substring( 0, reg.start() );

            if ( qry.substring( qry.length() ) == " " )
                qry += "limit 0";
            else
                qry += " limit 0";
        }

        return qry;
    }
};

//
public class hive implements SQLData 
{
    //
    private String sql_;        // SQL type name
    private BigDecimal key_;    // Context key

    //
    final static BigDecimal SUCCESS = new BigDecimal( 0 );
    final static BigDecimal FAILURE = new BigDecimal( 1 );

    // override (SQLData inheritence)
    public String getSQLTypeName()
        throws SQLException 
    {
        return sql_;
    }

    // override (SQLData inheritence)
    public void readSQL( SQLInput stream, String type )
        throws SQLException 
    {
        sql_ = type;
        key_ = stream.readBigDecimal();
    }

    // override (SQLData inheritence)
    public void writeSQL( SQLOutput stream )
        throws SQLException 
    {
        stream.writeBigDecimal( key_ );
    }

    //
    static public BigDecimal ODCITableDescribe( STRUCT[] sctx, String stmt )
        throws SQLException, hive_exception
    {
        //String sql = "begin impl.initialize( ? ); end;";
        String sql = "begin anytype.begincreate( dbms_types.typecode_object, ? ); end;";

        hive_context ctx = new hive_context( stmt );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );
        OracleCallableStatement stm = null;

        ResultSetMetaData rmd = ctx.descSql();

        stm = (OracleCallableStatement)con.prepareCall( sql );
        stm.registerOutParameter( 1, OracleTypes.OPAQUE, "ANYTYPE" );
        stm.execute();

        Object[] obj = new Object[ 1 ];
        obj[ 0 ] = stm.getObject( 1 );
        
        if ( rmd.getColumnCount() > 0 )
        {
            for ( int i = 1; i <= rmd.getColumnCount(); ++i ) 
            {
                sql = "begin impl.attribute( ?, ?, ?, ?, ?, ?, ?, ?, ? ); end;";
                stm = (OracleCallableStatement)con.prepareCall( sql );

                //
                stm.registerOutParameter( 1, OracleTypes.OPAQUE, "ANYTYPE" );
                stm.setObject( 1, obj[ 0 ] );

                //
                //System.out.format( "Processing column [%s]: ", rmd.getColumnName( i ) );
                stm.setString( 2, rmd.getColumnName( i ) );

                //
                switch ( rmd.getColumnType( i ) )
                {
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.CHAR:
                    case java.sql.Types.NVARCHAR:
                    case java.sql.Types.NCHAR:
                        {
                            //System.out.format( "%s\n", "DBMS_TYPES.TYPECODE_VARCHAR2" );
                            stm.setInt( 3, DBMS_TYPES.TYPECODE_VARCHAR2 );

                            stm.setNull( 4, java.sql.Types.INTEGER );
                            stm.setNull( 5, java.sql.Types.INTEGER );

                            if ( rmd.getPrecision( i ) > 4000 )
                                stm.setInt( 6, 4000 );
                            else
                                stm.setInt( 6, rmd.getPrecision( i ) );

                            stm.setNull( 7, java.sql.Types.INTEGER );
                            stm.setNull( 8, java.sql.Types.INTEGER );
                            stm.setNull( 9, java.sql.Types.INTEGER );
                        }
                        break;

                    case java.sql.Types.BIGINT:
                    case java.sql.Types.DOUBLE:
                    case java.sql.Types.FLOAT:
                    case java.sql.Types.INTEGER:
                    case java.sql.Types.NUMERIC:
                    case java.sql.Types.REAL:
                    case java.sql.Types.SMALLINT:
                    case java.sql.Types.TINYINT:
                    case java.sql.Types.DECIMAL:
                    case java.sql.Types.BOOLEAN:
                        {
                            //System.out.format( "%s\n", "DBMS_TYPES.TYPECODE_NUMBER" );
                            stm.setInt( 3, DBMS_TYPES.TYPECODE_NUMBER );

                            stm.setNull( 4, java.sql.Types.INTEGER );
                            stm.setNull( 5, java.sql.Types.INTEGER );
                            stm.setNull( 6, java.sql.Types.INTEGER );
                            stm.setNull( 7, java.sql.Types.INTEGER );
                            stm.setNull( 8, java.sql.Types.INTEGER );
                            stm.setNull( 9, java.sql.Types.INTEGER );
                        }
                        break;

                    case java.sql.Types.CLOB:
                    case java.sql.Types.NCLOB:
                        {
                            //System.out.format( "%s\n", "DBMS_TYPES.TYPECODE_CLOB" );
                            stm.setInt( 3, DBMS_TYPES.TYPECODE_CLOB );

                            stm.setNull( 4, java.sql.Types.INTEGER );
                            stm.setNull( 5, java.sql.Types.INTEGER );
                            stm.setNull( 6, java.sql.Types.INTEGER );
                            stm.setNull( 7, java.sql.Types.INTEGER );
                            stm.setNull( 8, java.sql.Types.INTEGER );
                            stm.setNull( 9, java.sql.Types.INTEGER );
                        }
                        break;

                    case java.sql.Types.BLOB:
                        {
                            //System.out.format( "%s\n", "DBMS_TYPES.TYPECODE_BLOB" );
                            stm.setInt( 3, DBMS_TYPES.TYPECODE_BLOB );

                            stm.setNull( 4, java.sql.Types.INTEGER );
                            stm.setNull( 5, java.sql.Types.INTEGER );
                            stm.setNull( 6, java.sql.Types.INTEGER );
                            stm.setNull( 7, java.sql.Types.INTEGER );
                            stm.setNull( 8, java.sql.Types.INTEGER );
                            stm.setNull( 9, java.sql.Types.INTEGER );
                        }
                        break;

                    case java.sql.Types.DATE:
                    case java.sql.Types.TIME:
                    case java.sql.Types.TIMESTAMP:
                        {
                            //System.out.format( "%s\n", "DBMS_TYPES.TYPECODE_DATE" );
                            stm.setInt( 3, DBMS_TYPES.TYPECODE_DATE );

                            stm.setNull( 4, java.sql.Types.INTEGER );
                            stm.setNull( 5, java.sql.Types.INTEGER );
                            stm.setNull( 6, java.sql.Types.INTEGER );
                            stm.setNull( 7, java.sql.Types.INTEGER );
                            stm.setNull( 8, java.sql.Types.INTEGER );
                            stm.setNull( 9, java.sql.Types.INTEGER );
                        }
                        break;

                    case java.sql.Types.LONGNVARCHAR:
                    case java.sql.Types.LONGVARBINARY:
                    case java.sql.Types.LONGVARCHAR:
                    case java.sql.Types.ARRAY:
                    case java.sql.Types.BINARY:
                    case java.sql.Types.BIT:
                    case java.sql.Types.DATALINK:
                    case java.sql.Types.DISTINCT:
                    case java.sql.Types.JAVA_OBJECT:
                    case java.sql.Types.NULL:
                    case java.sql.Types.OTHER:
                    case java.sql.Types.REF:
                    case java.sql.Types.ROWID:
                    case java.sql.Types.SQLXML:
                    case java.sql.Types.STRUCT:
                    case java.sql.Types.VARBINARY:
                    default:
                        {
                            //System.out.format( "%s\n", "NULL" );
                            stm.setNull( 3, java.sql.Types.INTEGER );
                            stm.setNull( 4, java.sql.Types.INTEGER );
                            stm.setNull( 5, java.sql.Types.INTEGER );
                            stm.setNull( 6, java.sql.Types.INTEGER );
                            stm.setNull( 7, java.sql.Types.INTEGER );
                            stm.setNull( 8, java.sql.Types.INTEGER );
                            stm.setNull( 9, java.sql.Types.INTEGER );
                        }
                        break;
                }

                //
                stm.execute();
                obj[ 0 ] = stm.getObject( 1 );
            }

            //
            sql = "begin impl.finalize( ? ); end;";
            stm = (OracleCallableStatement)con.prepareCall( sql );
            stm.registerOutParameter( 1, OracleTypes.OPAQUE, "ANYTYPE" );
            stm.setObject( 1, obj[ 0 ] );
            stm.execute();

            obj[ 0 ] = stm.getObject( 1 );

            StructDescriptor dsc = new StructDescriptor( "ANYTYPE", con );
            sctx[ 0 ] = new STRUCT( dsc, con, obj );
        }

        return SUCCESS;
    }

    //
    static public BigDecimal ODCITableStart( STRUCT[] sctx, String stmt )
        throws SQLException, hive_exception
    {
        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        // create context and result
        hive_context ctx = new hive_context( stmt );

        // register stored context with cartridge services
        int key;

        try
        {
            key = ContextManager.setContext( ctx );
        }
        catch ( CountException ex )
        {
            return FAILURE;
        }

        //
        Object[] imp = new Object[ 1 ];
        imp[ 0 ] = new BigDecimal( key );

        StructDescriptor dsc = new StructDescriptor( "HIVE_T", con );
        sctx[ 0 ] = new STRUCT( dsc, con, imp );

        return SUCCESS;
    }

    static public BigDecimal ODCITableFetch( BigDecimal key, BigDecimal max, java.sql.Array[] out )
        throws SQLException, InvalidKeyException, hive_exception
    {
        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );
        hive_context ctx = (hive_context)ContextManager.getContext( key.intValue() );

        //
        if ( ! ctx.ready() )
        {
            if ( ! ctx.execute() )
                return FAILURE;
        }

        //
        StructDescriptor dsc = new StructDescriptor( "columns_t", con );

        //
        for ( int i = 0; i < max.intValue(); ++i )
        {
            if ( ctx.next() )
            {
                Object[] cols = new Object[ ctx.columnCount() ];

                for ( int c = 1; i <= ctx.columnCount(); ++c )
                {
                    Object col = ctx.getObject( i );
                    int typ = ( col instanceof Timestamp ) ? 91 : ctx.columnType( i );

                    Object[] atr =                      // column_t
                    {
                        new BigDecimal( typ ),          // typecode
                        ( typ == 12 )   ? col : null,   // v2_column
                        ( typ == 2 )    ? col : null,   // num_column
                        ( typ == 91 )   ? col : null,   // date_column
                        ( typ == 2005 ) ? col : null,   // clob_column
                        null,                           // raw_column
                        null,                           // raw_error
                        null,                           // raw_length
                        null,                           // ids_column
                        null,                           // iym_column
                        ( typ == 93 )   ? col : null,   // ts_column
                        ( typ == -101 ) ? col : null,   // tstz_column
                        ( typ == -102 ) ? col : null,   // tsltz_column
                        new Integer( 0 ),               // cvl_offset
                        null                            // cvl_length
                    };

                    //
                    cols[ i ] = new STRUCT( dsc, con, atr );
                }

                //
                ArrayDescriptor ary = ArrayDescriptor.createDescriptor( "row_t", con );

                ARRAY arr = new ARRAY( ary, con, cols );
                out[ 0 ] = arr;
            }
            else
                out[ 0 ] = null;
        }

        return SUCCESS;
    }

    //
    static public BigDecimal ODCITableClose( BigDecimal key )
        throws SQLException, InvalidKeyException
    {
        hive_context ctx = (hive_context)ContextManager.clearContext( key.intValue() );
        ctx.clear();

        return SUCCESS;
    }

};
/

--
show errors

--
-- ... done!
--
