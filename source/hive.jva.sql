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
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
                ret = TYPECODE_DATE;
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
        System.out.println( "hive_context ctor: " + sql );
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
        System.out.println( "hive_context ready: " + ( ! ( rst_ == null ) ) );
        return ( ! ( rst_ == null ) );
    }

    //
    public void clear()
    {
        System.out.println( "hive_context clear" );
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

        System.out.println( "hive_context columnCount rmd_: " + rmd_ );
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

        System.out.println( "hive_context columnType rmd_: " + rmd_ );
        return rmd_.getColumnType( i );
    }

    // recordset
    //
    public boolean next() throws SQLException
    {
        System.out.println( "hive_context next rst_: " + rst_ );
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
        System.out.println( "hive_context execute" );

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        System.out.println( "hive_context execute" );
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

        System.out.println( "hive_context setPreparedStatement returns: " + ( ! ( stm_ == null ) ) );
        return ( ! ( stm_ == null ) );
    }

    //
    private boolean setResultSet() throws SQLException, hive_exception
    {
        System.out.println( "hive_context setResultSet" );

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( rst_ == null )
        {
            if ( setPreparedStatement() )
                rst_ = stm_.executeQuery();
        }

        System.out.println( "hive_context setResultSet returns: " + ( ! ( rst_ == null ) ) );
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

        System.out.println( "hive_context setResultSetMetaData returns: " + ( ! ( rmd_ == null ) ) );
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

            System.out.println( "hive_manager new map size: " + map_.size() );
        }
        else
            System.out.println( "hive_manager found existing context" );

        System.out.println( "hive_manager createContext return: " + key );
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
        System.out.println( "getSQLTypeName called" );
        return sql_;
    }

    // override (SQLData inheritence)
    public void readSQL( SQLInput stream, String type )
        throws SQLException 
    {
        System.out.println( "readSQL called" );
        sql_ = type;
        key_ = stream.readBigDecimal();
    }

    // override (SQLData inheritence)
    public void writeSQL( SQLOutput stream )
        throws SQLException 
    {
        System.out.println( "writeSQL called" );
        stream.writeBigDecimal( key_ );
    }

    //
    static public BigDecimal SqlDesc( String stmt, oracle.sql.ARRAY[] attr )
        throws SQLException, hive_exception
    {
        System.out.println( "SqlDesc called" );

        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = new hive_context( stmt );

        if ( ctx == null )
            throw new hive_exception( "Context not created for SqlDesc" );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        ResultSetMetaData rmd = ctx.descSql();
        System.out.println( "SqlDesc: columns: " + rmd.getColumnCount() );

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
    static public BigDecimal SqlDesc( BigDecimal key, oracle.sql.ARRAY[] attr )
        throws SQLException, hive_exception
    {
        System.out.println( "SqlDesc called" );

        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = manager_.getContext( key );

        if ( ctx == null )
            throw new hive_exception( "Context not found for SqlDesc" );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        ResultSetMetaData rmd = ctx.descSql();
        System.out.println( "SqlDesc: columns: " + rmd.getColumnCount() );

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
        System.out.println( "SqlOpen called" );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            System.out.println( "SqlOpen created hive_manager" );
        }

        hive_context ctx = new hive_context( stmt );
        key_ = manager_.createContext( ctx );

        System.out.println( "SqlOpen key: " + key_ );

        return key_;
    }

    //
    static public BigDecimal SqlOpen( STRUCT[] sctx, String stmt )
        throws SQLException, hive_exception
    {
        System.out.println( "SqlOpen called" );
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
        System.out.println( "SqlOpen called" );

        key_ = SqlOpen( stmt );

        if ( key_.intValue() == 0 )
            return FAILURE;

        key[ 0 ] = key_;

        return SUCCESS;
    }

    static public BigDecimal SqlFetch( BigDecimal key, BigDecimal num, ARRAY[] out )
        throws SQLException, InvalidKeyException, hive_exception
    {
        System.out.println( "SqlFetch called: key_ = " + key );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            System.out.println( "SqlFetch created hive_manager" );
        }

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

        //
        System.out.println( "SqlFetch requesting the next " + num + " record(s)" );
        for ( int i = 0; i < num.intValue(); ++i )
        {
            if ( ctx.next() )
            {
                int cnt = ctx.columnCount();
                Object[] cols = new Object[ cnt ];

                System.out.println( "SqlFetch processing " + cnt + " column(s)" );

                for ( int c = 1; c <= cnt; ++c )
                {
                    Object col = ctx.getObject( c );
                    int typ = DBMS_TYPES.to_dbms_type( ctx.columnType( c ) );

                    Object[] atr =
                    {
                        new BigDecimal( typ ),                                  // code
                        ( typ == DBMS_TYPES.TYPECODE_VARCHAR2 ) ? col : null,   // val_varchar2
                        ( typ == DBMS_TYPES.TYPECODE_NUMBER )   ? col : null,   // val_number
                        ( typ == DBMS_TYPES.TYPECODE_DATE )     ? col : null,   // val_date
                        ( typ == DBMS_TYPES.TYPECODE_CLOB )     ? col : null,   // val_clob
                        ( typ == DBMS_TYPES.TYPECODE_BLOB )     ? col : null    // val_blob
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
        }

        return SUCCESS;
    }

    //
    static public BigDecimal SqlClose( BigDecimal key )
        throws SQLException, InvalidKeyException
    {
        System.out.println( "SqlClose called" );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            System.out.println( "SqlClose created hive_manager" );
        }

        hive_context ctx = manager_.removeContext( key );

        if ( ctx != null )
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
