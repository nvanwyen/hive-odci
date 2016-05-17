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
public class debug
{
    //
    static void log( String msg )
    {
        // log to db
    }

    //
    static void output( String msg )
    {
        System.out.println( msg );
    }
};

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

    //
    static public int to_jdbc_type( int typ )
    {
        int ret = 0;

        //
        switch ( typ )
        {
            case TYPECODE_VARCHAR2:
                ret = java.sql.Types.VARCHAR;
                break;

            case TYPECODE_NUMBER:
                ret = java.sql.Types.INTEGER;
                break;

            case TYPECODE_CLOB:
                ret = java.sql.Types.CLOB;
                break;

            case TYPECODE_BLOB:
                ret = java.sql.Types.BLOB;
                break;

            case TYPECODE_DATE:
                ret = java.sql.Types.DATE;
                break;

            case TYPECODE_OBJECT:
            default:
                ret = java.sql.Types.STRUCT;
                break;
        }

        //
        return ret;
    }

    //
    static public int to_dbms_type( int typ )
    {
        int ret = 0;

        //
        switch ( typ )
        {
            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.NCHAR:
                ret = TYPECODE_VARCHAR2;
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
                ret = TYPECODE_NUMBER;
                break;

            case java.sql.Types.CLOB:
            case java.sql.Types.NCLOB:
                ret = TYPECODE_CLOB;
                break;

            case java.sql.Types.BLOB:
                ret = TYPECODE_BLOB;
                break;

            case java.sql.Types.DATE:
                ret = TYPECODE_DATE;
                break;

            case java.sql.Types.TIMESTAMP:
                ret = TYPECODE_TIMESTAMP;
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
                ret = TYPECODE_OBJECT; // ? unknown ?
                break;
        }

        //
        return ret;
    }

    //
    public static int nls_charset_id()
    {
        int id = 0;

        //
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            String sql = "select nls_charset_id( property_value ) id " +
                           "from database_properties " +
                          "where property_name in ( 'NLS_CHARACTERSET' )";

            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            stm = con.prepareStatement( sql );
            ResultSet rst = stm.executeQuery();

            if ( rst.next() )
                id = rst.getInt( 1 );

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
        return id;
    }

    //
    public static int nls_charset_format()
    {
        return 1;      // always 1
    }
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

//
public class hive_session
{
    String sql_;

    public String host;
    public String port;
    public String name;
    public String pass;

    //
    public hive_session()
    {
        host = "";
        port = "";
        name = "";
        pass = "";
    }

    //
    public hive_session( String h, String p, String n, String w )
    {
        host = h;
        port = p;
        name = n;
        pass = w;
    }

    //
    public hive_session( oracle.sql.STRUCT obj )
        throws SQLException
    {
        if ( obj != null )
        {
            oracle.sql.Datum[] atr = obj.getOracleAttributes();

            if ( atr.length > 0 )
            {
                if ( atr[ 0 ] != null )
                    host = atr[ 0 ].toString();
                else
                    host = "";
            }
            else
                host = "";

            if ( atr.length > 1 )
            {
                if ( atr[ 1 ] != null )
                    port = atr[ 1 ].toString();
                else
                    port = "";
            }
            else
                port = "";

            if ( atr.length > 2 )
            {
                if ( atr[ 2 ] != null )
                    name = atr[ 2 ].toString();
                else
                    name = "";
            }
            else
                name = "";

            if ( atr.length > 3 )
            {
                if ( atr[ 3 ] != null )
                    pass = atr[ 3 ].toString();
                else
                    pass = "";
            }
            else
                pass = "";
        }
    }

    //
    public String toString()
    {
        String str = "";

        str += "host: " + host + "\n";
        str += "port: " + port + "\n";
        str += "name: " + name + "\n";
        str += "pass: " + pass + "\n";

        return str;
    }
};

//
public class hive_bind
{
    //
    public static final long UNKNOWN        =  0;
    //
    public static final long REF_IN         =  1;
    public static final long REF_OUT        =  2;
    public static final long REF_INOUT      =  3;
    //
    public static final long TYPE_BOOL      =  1;
    public static final long TYPE_DATE      =  2;
    public static final long TYPE_FLOAT     =  3;
    public static final long TYPE_INT       =  4;
    public static final long TYPE_LONG      =  5;
    public static final long TYPE_NULL      =  6;
    public static final long TYPE_ROWID     =  7;
    public static final long TYPE_SHORT     =  8;
    public static final long TYPE_STRING    =  9;
    public static final long TYPE_TIME      = 10;
    public static final long TYPE_TIMESTAMP = 11;
    public static final long TYPE_URL       = 12;

    //
    public String value;
    public long   type;
    public long   scope;

    //
    public hive_bind()
    {
        value = "";
        type  = UNKNOWN;
        scope = UNKNOWN;
    }

    //
    public hive_bind( String v, long t, long s )
    {
        value = v;
        type  = t;
        scope = s;
    }

    //
    public hive_bind( oracle.sql.STRUCT obj )
        throws SQLException
    {
        if ( obj != null )
        {
            oracle.sql.Datum[] atr = obj.getOracleAttributes();

            if ( atr.length > 0 )
            {
                if ( atr[ 0 ] != null )
                    value = atr[ 0 ].toString();
                else
                    value = "";
            }
            else
                value = "";

            if ( atr.length > 1 )
            {
                if ( atr[ 1 ] != null )
                    type = atr[ 1 ].longValue();
                else
                    type = UNKNOWN;
            }
            else
                type = UNKNOWN;

            if ( atr.length > 2 )
            {
                if ( atr[ 2 ] != null )
                    scope = atr[ 2 ].longValue();
                else
                    scope = UNKNOWN;
            }
            else
                scope = UNKNOWN;
        }
    }

    //
    public String toString()
    {
        String str = "";

        str += "value: " + value + "\n";
        str += "type:  " + type  + "\n";
        str += "scope: " + scope + "\n";

        return str;
    }
};

//
public class hive_bindings
{
    public ArrayList<hive_bind> binds;

    //
    public hive_bindings()
    {
        binds = new ArrayList<hive_bind>();
    }

    //
    public hive_bindings( ArrayList<hive_bind> b )
    {
        binds = b;
    }

    //
    public hive_bindings( oracle.sql.ARRAY obj )
        throws SQLException
    {
        binds = new ArrayList<hive_bind>();

        if ( obj != null )
        { 
            Datum[] dat = obj.getOracleArray();

            for ( int i = 0; i < dat.length; ++i )
            {
                if ( dat[ i ] != null )
                    binds.add( new hive_bind( (STRUCT)dat[ i ] ) );
            }
        }
    }

    //
    public String toString()
    {
        String str = "";

        for ( hive_bind bnd : binds )
            str += bnd.toString();

        return str;
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
    public hive_session session;

    //
    Connection conn_;

    //
    public hive_connection()
    {
        session = new hive_session();
    }

    //
    public hive_connection( hive_session con )
    {
        session = con;
    }

    //
    public hive_connection( String host, String port )
    {
        session = new hive_session();

        session.host = host;
        session.port = port;
    }

    //
    public hive_connection( String host, String port, String name, String pass )
    {
        session = new hive_session();

        session.host = host;
        session.port = port;
        session.name = name;
        session.pass = pass;
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
    public void setSession( hive_session val ) { session = val; }
    public hive_session getSession() { return session; }

    //
    public void setHost( String val ) { session.host = val; }
    public void setPort( String val ) { session.port = val; }
    public void setUser( String val ) { session.name = val; }
    public void setPass( String val ) { session.pass = val; }

    //
    public String getHost() { return session.host; }
    public String getPort() { return session.port; }
    public String getUser() { return session.name; }
    public String getPass() { return session.pass; }

    //
    public String getUrl() throws hive_exception
    {
        if ( ( session.host.length() > 0 )
          && ( session.port.length() > 0 )
          && ( session.name.length() > 0 )
          && ( session.pass.length() > 0 ) )
        {
            if ( url_.indexOf( '%' ) >= 0 )
            {
                url_.replace( "%h%", session.host );
                url_.replace( "%p%", session.port );
                url_.replace( "%u%", session.name );
                url_.replace( "%w%", session.pass );
            }
        }
        else
        {
            if ( session.host.length() == 0 )
                throw new hive_exception( "Missing host in connection data" );

            if ( session.port.length() == 0 )
                throw new hive_exception( "Missing port in connection data" );

            if ( session.name.length() == 0 )
                throw new hive_exception( "Missing user in connection data" );

            if ( session.pass.length() == 0 )
                throw new hive_exception( "Missing password in connection data" );
        }

        //
        url_ = "jdbc:datadirect:hive://orabdc.local:10000;User=oracle;Password=welcome1";
        //debug.output( url_ );
        return url_;
    }

    //
    public boolean loadConnection()
    {
        if ( session.host.length() == 0 )
            session.host = "orabdc.local"; // param_.value( "default_hive_host" );

        if ( session.port.length() == 0 )
            session.port = "10000"; // param_.value( "default_hive_port" );

        if ( session.name.length() == 0 )
            session.name = "oracle"; // param_.value( "default_hive_user" );

        if ( session.pass.length() == 0 )
            session.pass = "welcome1"; // param_.value( "default_hive_pass" );

        return ( ( session.host.length() > 0 )
              && ( session.port.length() > 0 )
              && ( session.name.length() > 0 )
              && ( session.pass.length() > 0 ) );
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

    //
    private long rec_;

    // ctor
    //
    public hive_context( String sql ) throws SQLException, hive_exception
    {
        //debug.output( "hive_context ctor: " + sql );
        sql_ = sql;

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( hcn_ == null )
            hcn_ = new hive_connection();

        hcn_.loadDriver();
        hcn_.createConnection();

        rec_ = 0;
    }

    //
    public boolean ready()
    {
        //debug.output( "hive_context ready: " + ( ! ( rst_ == null ) ) );
        return ( ! ( rst_ == null ) );
    }

    //
    public void clear()
    {
        //debug.output( "hive_context clear" );
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
    public int columnCount() throws SQLException, hive_exception
    {
        if ( rmd_ == null )
        {
            if ( rst_ == null )
                setResultSet();

            rmd_ = rst_.getMetaData();
        }

        //debug.output( "hive_context columnCount rmd_: " + rmd_ );
        return rmd_.getColumnCount();
    }

    //
    public int columnType( int i ) throws SQLException, hive_exception
    {
        if ( rmd_ == null )
        {
            if ( rst_ == null )
                setResultSet();

            rmd_ = rst_.getMetaData();
        }

        //debug.output( "hive_context columnType rmd_: " + rmd_ );
        return rmd_.getColumnType( i );
    }

    // recordset
    //
    public boolean next() throws SQLException
    {
        ++rec_;

        //debug.output( "hive_context next rst_: " + rst_ );
        return rst_.next();
    }

    public long fetched()
    {
        return rec_;
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
        //debug.output( "hive_context execute" );

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

        //debug.output( "hive_context setPreparedStatement returns: " + ( ! ( stm_ == null ) ) );
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

        //debug.output( "hive_context setResultSet returns: " + ( ! ( rst_ == null ) ) );
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

        //debug.output( "hive_context setResultSetMetaData returns: " + ( ! ( rmd_ == null ) ) );
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

// stored context manager (since the Oracle one is broken)
public class hive_manager
{
    private static BigDecimal key_;
    private static HashMap<BigDecimal, hive_context> map_;

    //
    public hive_manager()
    {
        key_ = new BigDecimal( 0 );
        map_ = new HashMap<BigDecimal, hive_context>();
    }

    //
    private BigDecimal nextKey()
    {
        key_ = key_.add( new BigDecimal( 1 ) );
        return key_;
    }

    //
    public BigDecimal findContext( hive_context ctx )
    {
        BigDecimal key = new BigDecimal( 0 );

        for ( Map.Entry<BigDecimal, hive_context> ent : map_.entrySet() )
        {
            hive_context itm = ent.getValue();

            if ( itm.getSql().equals( ctx.getSql() ) )
            {
                key = ent.getKey();
                break;
            }
        }

        return key;
    }

    //
    public BigDecimal createContext( hive_context ctx )
    {
        BigDecimal key = findContext( ctx );

        if ( key.intValue() == 0 )
        {
            key = nextKey();
            map_.put( key, ctx );

            //debug.output( "hive_manager new map size: " + map_.size() );
        }
        else
        {
            //debug.output( "hive_manager found existing context" );
        }

        //debug.output( "hive_manager createContext return: " + key );
        return key;
    }

    //
    public hive_context getContext( BigDecimal key )
        throws hive_exception
    {
        hive_context ctx = map_.get( key );

        if ( ctx == null )
            throw new hive_exception( "Invalid context key: " + key.intValue() );

        return ctx;
    }

    //
    public hive_context removeContext( BigDecimal key )
    {
        hive_context ctx = null;

        try
        {
            ctx = getContext( key );

            if ( ctx != null )
                map_.remove( key );
        }
        catch ( hive_exception ex )
        {
            // nothing to do ...
        }

        return ctx;
    }
};

//
public class hive implements SQLData
{
    //
    public static class attribute
    {
        public String name;
        public int    code;
        public int    prec;
        public int    scale;
        public int    len;
        public int    csid;
        public int    csfrm;

        attribute()
        {
            name  = "";
            code  = -1;
            prec  = -1;
            scale = -1;
            len   = -1;
            csid  = -1;
            csfrm = -1;
        }

        public String toString()
        {
            String str = new String();

            str +=                          "\n";
            str += "... name:  " +  name  + "\n";
            str += "... code:  " +  code  + "\n";
            str += "... prec:  " +  prec  + "\n";
            str += "... scale: " +  scale + "\n";
            str += "... len:   " +  len   + "\n";
            str += "... csid:  " +  csid  + "\n";
            str += "... csfrm: " +  csfrm + "\n";

            return str;
        }
    };

    //
    private static hive_manager manager_;

    //
    private static String sql_;        // needed for SQLData inheritence
    private static BigDecimal key_;    //

    //
    final static BigDecimal SUCCESS = new BigDecimal( 0 );
    final static BigDecimal FAILURE = new BigDecimal( 1 );

    // override (SQLData inheritence)
    public String getSQLTypeName()
        throws SQLException 
    {
        //debug.output( "getSQLTypeName called" );
        return sql_;
    }

    // override (SQLData inheritence)
    public void readSQL( SQLInput stream, String type )
        throws SQLException 
    {
        //debug.output( "readSQL called" );
        sql_ = type;
        key_ = stream.readBigDecimal();
    }

    // override (SQLData inheritence)
    public void writeSQL( SQLOutput stream )
        throws SQLException 
    {
        //debug.output( "writeSQL called" );
        stream.writeBigDecimal( key_ );
    }

    //
    static public BigDecimal SqlDesc( String stmt, oracle.sql.ARRAY[] attr )
        throws SQLException, hive_exception
    {
        //debug.output( "SqlDesc called" );

        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = new hive_context( stmt );

        if ( ctx == null )
            throw new hive_exception( "Context not created for SqlDesc" );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        ResultSetMetaData rmd = ctx.descSql();
        //debug.output( "SqlDesc: columns: " + rmd.getColumnCount() );

        if ( rmd.getColumnCount() > 0 )
        {
            int cset = DBMS_TYPES.nls_charset_id();
            int cfrm = DBMS_TYPES.nls_charset_format();

            for ( int i = 1; i <= rmd.getColumnCount(); ++i ) 
            {
                attribute atr = new attribute();

                atr.name = rmd.getColumnName( i );
                atr.code = DBMS_TYPES.to_dbms_type( rmd.getColumnType( i ) );

                switch ( atr.code )
                {
                    case DBMS_TYPES.TYPECODE_VARCHAR2:
                        {
                            if ( rmd.getPrecision( i ) > 4000 )
                                atr.len = 4000;
                            else
                            {
                                if ( rmd.getPrecision( i ) > 0 )
                                    atr.len = rmd.getPrecision( i );
                                else
                                    atr.len = -1;
                            }
                        }
                        atr.csid = cset;
                        atr.csfrm = cfrm;
                        atr.prec = -1;
                        atr.scale = -1;
                        break;

                    case DBMS_TYPES.TYPECODE_NUMBER:
                        atr.prec = rmd.getPrecision( i );
                        atr.scale = rmd.getScale( i );

                        if ( ( atr.prec == 0 ) && ( atr.scale == 0 ) )
                            atr.scale = -127;

                        if ( atr.prec == -1 ) 
                        {
                            atr.prec = 0;
                            atr.scale = -127;
                        }

                        if ( atr.scale == -1 )
                            atr.scale = -127;

                        break;

                    case DBMS_TYPES.TYPECODE_CLOB:
                        atr.len = rmd.getPrecision( i );
                        atr.csid = cset;
                        atr.csfrm = cfrm;
                        break;

                    case DBMS_TYPES.TYPECODE_BLOB:
                        atr.len = rmd.getPrecision( i );
                        break;

                    case DBMS_TYPES.TYPECODE_DATE:
                        break;

                    case DBMS_TYPES.TYPECODE_TIMESTAMP:
                    case DBMS_TYPES.TYPECODE_TIMESTAMP_TZ:
                    case DBMS_TYPES.TYPECODE_TIMESTAMP_LTZ:
                        atr.prec = 0;
                        atr.scale = 6;
                        break;

                    case DBMS_TYPES.TYPECODE_OBJECT:
                    default:
                        break;
                }

                Object[] obj = new Object[] { new String( atr.name ),
                                              new Integer( atr.code ),
                                              new Integer( atr.prec ),
                                              new Integer( atr.scale ),
                                              new Integer( atr.len ),
                                              new Integer( atr.csid ),
                                              new Integer( atr.csfrm ) };

                StructDescriptor ids = StructDescriptor.createDescriptor( "ATTRIBUTE", con );
                STRUCT itm = new STRUCT( ids, con, obj );
                col.add( itm );
            }
        }

        ArrayDescriptor des = ArrayDescriptor.createDescriptor( "ATTRIBUTES", con );
        STRUCT[] dat = col.toArray( new STRUCT[ col.size() ] );

        attr[0] = new ARRAY( des, con, dat );

        return SUCCESS;
    }

    //
    static public BigDecimal SqlDesc( BigDecimal key, oracle.sql.ARRAY[] attr )
        throws SQLException, hive_exception
    {
        //debug.output( "SqlDesc called" );

        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = manager_.getContext( key );

        if ( ctx == null )
            throw new hive_exception( "Context not found for SqlDesc" );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        ResultSetMetaData rmd = ctx.descSql();
        //debug.output( "SqlDesc: columns: " + rmd.getColumnCount() );

        if ( rmd.getColumnCount() > 0 )
        {
            for ( int i = 1; i <= rmd.getColumnCount(); ++i ) 
            {
                attribute atr = new attribute();

                atr.name = rmd.getColumnName( i );
                atr.code = DBMS_TYPES.to_dbms_type( rmd.getColumnType( i ) );

                switch ( atr.code )
                {
                    case DBMS_TYPES.TYPECODE_VARCHAR2:
                        {
                            if ( rmd.getPrecision( i ) > 4000 )
                                atr.len = 4000;
                            else
                            {
                                if ( rmd.getPrecision( i ) > 0 )
                                    atr.len = rmd.getPrecision( i );
                                else
                                    atr.len = -1;
                            }
                        }
                        break;

                    case DBMS_TYPES.TYPECODE_NUMBER:
                        atr.prec = rmd.getPrecision( i );
                        atr.scale = rmd.getScale( i );
                        break;

                    case DBMS_TYPES.TYPECODE_CLOB:
                        atr.len = rmd.getPrecision( i );
                        break;

                    case DBMS_TYPES.TYPECODE_BLOB:
                        atr.len = rmd.getPrecision( i );
                        break;

                    case DBMS_TYPES.TYPECODE_DATE:
                        break;

                    case DBMS_TYPES.TYPECODE_OBJECT:
                    default:
                        break;
                }

                Object[] obj = new Object[] { new String( atr.name ),
                                              new Integer( atr.code ),
                                              new Integer( atr.prec ),
                                              new Integer( atr.scale ),
                                              new Integer( atr.len ),
                                              new Integer( atr.csid ),
                                              new Integer( atr.csfrm ) };

                StructDescriptor ids = StructDescriptor.createDescriptor( "ATTRIBUTE", con );
                STRUCT itm = new STRUCT( ids, con, obj );
                col.add( itm );
            }
        }

        ArrayDescriptor des = ArrayDescriptor.createDescriptor( "ATTRIBUTES", con );
        STRUCT[] dat = col.toArray( new STRUCT[ col.size() ] );

        attr[0] = new ARRAY( des, con, dat );

        return SUCCESS;
    }

    //
    static public BigDecimal SqlOpen( String stmt )
        throws SQLException, hive_exception
    {
        //debug.output( "SqlOpen called [String]: " + stmt );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            //debug.output( "SqlOpen created hive_manager" );
        }

        hive_context ctx = new hive_context( stmt );
        key_ = manager_.createContext( ctx );

        //debug.output( "SqlOpen returning key: " + key_ );

        return key_;
    }

    //
    static public BigDecimal SqlOpen( STRUCT[] sctx, String stmt )
        throws SQLException, hive_exception
    {
        //debug.output( "SqlOpen called [STRUCT]: " + sctx );
        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        //
        key_ = SqlOpen( stmt );

        if ( key_.intValue() == 0 )
            return FAILURE;

        //
        Object[] imp = new Object[ 1 ];
        imp[ 0 ] = key_;

        StructDescriptor dsc = new StructDescriptor( "HIVE_T", con );
        sctx[ 0 ] = new STRUCT( dsc, con, imp );

        return SUCCESS;
    }

    //
    static public BigDecimal SqlOpen( String stmt, BigDecimal[] key )
        throws SQLException, hive_exception
    {
        //debug.output( "SqlOpen called" );

        key_ = SqlOpen( stmt );

        if ( key_.intValue() == 0 )
            return FAILURE;

        key[ 0 ] = key_;

        return SUCCESS;
    }

    static public BigDecimal SqlFetch( BigDecimal key, BigDecimal num, ARRAY[] out )
        throws SQLException, InvalidKeyException, hive_exception
    {
        //debug.output( "SqlFetch called: key_ = " + key );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        if ( manager_ == null )
            manager_ = new hive_manager();

        hive_context ctx = manager_.getContext( key );

        if ( ctx == null )
            throw new hive_exception( "Context not found for SqlFetch" );

        //
        if ( ! ctx.ready() )
        {
            if ( ! ctx.execute() )
                return FAILURE;
        }

        //
        StructDescriptor dsc = new StructDescriptor( "DATA", con );

        if ( ctx.next() )
        {
            int cnt = ctx.columnCount();
            Object[] cols = new Object[ cnt ];

            for ( int c = 1; c <= cnt; ++c )
            {
                Object col = ctx.getObject( c );
                int typ = DBMS_TYPES.to_dbms_type( ctx.columnType( c ) );

                Object[] atr =
                {
                    new BigDecimal( typ ),                                  // type code
                    ( typ == DBMS_TYPES.TYPECODE_VARCHAR2 )  ? col : null,  // val_varchar2
                    ( typ == DBMS_TYPES.TYPECODE_NUMBER )    ? col : null,  // val_number
                    ( typ == DBMS_TYPES.TYPECODE_DATE )      ? col : null,  // val_date
                    ( typ == DBMS_TYPES.TYPECODE_TIMESTAMP ) ? col : null,  // val_timestamp
                    ( typ == DBMS_TYPES.TYPECODE_CLOB )      ? col : null,  // val_clob
                    ( typ == DBMS_TYPES.TYPECODE_BLOB )      ? col : null   // val_blob
                };

                //
                cols[ c - 1 ] = new STRUCT( dsc, con, atr );
            }

            //
            ArrayDescriptor ary = ArrayDescriptor.createDescriptor( "RECORDS", con );

            ARRAY arr = new ARRAY( ary, con, cols );
            out[ 0 ] = arr;
        }
        else
            out[ 0 ] = null;

        return SUCCESS;
    }

    //
    static public BigDecimal SqlClose( BigDecimal key )
        throws SQLException, InvalidKeyException
    {
        //debug.output( "SqlClose called" );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            //debug.output( "SqlClose created hive_manager" );
        }

        hive_context ctx = manager_.removeContext( key );

        if ( ctx != null )
            ctx.clear();

        return SUCCESS;
    }

/*
    //
    static public BigDecimal SqlConnection( STRUCT obj )
        throws SQLException, InvalidKeyException
    {
        System.out.println( "SqlConnection called" );

        if ( obj != null )
        {
            hive_session con = new hive_session( obj );
            System.out.println( con.toString() );

        }

        return new BigDecimal( 0 );
    }

    //
    static public BigDecimal SqlBinding( oracle.sql.ARRAY obj )
        throws SQLException, InvalidKeyException
    {
        System.out.println( "SqlBinding called" );

        if ( obj != null )
        {
            hive_bindings bnd = new hive_bindings( obj );
            System.out.println( bnd.toString() );
        }

        return new BigDecimal( 0 );
    }
*/
};
/

--
show errors

--
-- ... done!
--
