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
set define off

--
create or replace and compile java source named "hive" as

package oracle.mti;

//
import java.io.*;
import java.net.*;
import java.sql.*;
import java.math.*;
import java.util.*;
import java.text.*;
import java.util.regex.*;

import oracle.sql.*;
import oracle.jdbc.*;
import oracle.ODCI.*;
import oracle.CartridgeServices.*;

//
public class log
{
    //
    public static final int NONE            =  0;   // logging level
    public static final int ERROR           =  1;
    public static final int WARN            =  2;
    public static final int INFO            =  4;
    public static final int TRACE           =  8;

    //
    public static void write( int type, String text ) throws SQLException, Exception
    {
        //
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            if ( con.getAutoCommit() )
                con.setAutoCommit( false );

            //
            String sql = "begin impl.log( ?, ? ); end;";

            //
            stm = con.prepareStatement( sql );
            stm.setInt( 1, type );
            stm.setString( 2, text );

            //
            stm.executeUpdate();
            stm.close();
        }
        catch ( SQLException ex )
        {
            // ... do nothing!
        }
        catch ( Exception ex )
        {
            // ... do nothing!
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();

                if ( con != null )
                {
                    if ( ! con.getAutoCommit() )
                        con.setAutoCommit( true );
                }

                // *** do not close the "default" connection ***
            }
            catch ( SQLException ex )
            {
                // ... do nothing!
            }
        }
    }

    //
    public static void error( String text )
    {
        try { write( ERROR, text ); } catch ( Exception ex ) {}
    }

    //
    public static void warn( String text )
    {
        try { write( WARN, text ); } catch ( Exception ex ) {}
    }

    //
    public static void info( String text )
    {
        try { write( INFO, text ); } catch ( Exception ex ) {}
    }

    //
    public static void trace( String text )
    {
        try { write( TRACE, text ); } catch ( Exception ex ) {}
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
            String sql = "select sys_context( 'hivectx', substr( ?, 1, 30 ), 4000 ) value " +
                           "from dual";

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

            if ( val == null )
            {
                sql = "select value " +
                        "from param$ " +
                       "where name = ?";

                //
                stm = con.prepareStatement( sql );
                stm.setString( 1, name );

                //
                rst = stm.executeQuery();

                if ( rst.next() )
                    val = rst.getString( "value" );

                //
                rst.close();
                stm.close();
            }
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

    //
    static public String env( String name )
    {
        //
        String val = null;

        //
        Connection con = null;
        OracleCallableStatement stm = null;

        try
        {
            String sql = "begin sys.dbms_system.get_env( ?, ? ); end;";

            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            stm = (OracleCallableStatement)con.prepareCall( sql );
            stm.setString( 1, name );
            stm.registerOutParameter( 2, OracleTypes.VARCHAR );

            //
            stm.execute();
            val = stm.getString( 2 );
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

// define an empty callback handler, which sets both the
// username and password data to an empty string (as there
// is no ability to provide a prompted response from a user)
public class callback_handler implements CallbackHandler
{
    public void handle( Callback[] cb )
        throws IOException, UnsupportedCallbackException
    {
        for ( int i = 0; i < cb.length; i++ )
        {
            if ( cb[ i ] instanceof NameCallback )
            {
                NameCallback nc = (NameCallback)cb[ i ];
                nc.setName( "" );
            }
            else if ( cb[ i ] instanceof PasswordCallback )
            {
                PasswordCallback pc = (PasswordCallback)cb[ i ];
                pc.setPassword( ( new String( "" ) ).toCharArray() );
            }
            else
                throw new UnsupportedCallbackException( cb[ i ], "Unrecognised callback" );
        }
    }
};

//
public class hive_session
{
    String sql_;

    public String host;
    public String port;

    // auth type (kerberos or userIdPassword)
    public String auth;

    // when auth = userIdPassword
    public String name;
    public String pass;

    // when auth = kerberos
    public String kerb; // see also system paraemters: java.security.krb5.realm
                        //                             java.security.krb5.kdc
                        //                             java.security.krb5.conf
                        //                             java.security.auth.login.config

    //
    public hive_session()
    {
        auth = hive_parameter.value( "hive_auth" );

        if ( auth == null )
            auth = "userIdPassword";

        host = "";
        port = "";
        name = "";
        pass = "";
        kerb = "";
    }

    //
    public hive_session( String h, String p )
    {
        auth = hive_parameter.value( "hive_auth" );

        if ( auth == null )
            auth = "userIdPassword";

        host = h;
        port = p;
        name = "";
        pass = "";
        kerb = "";
    }

    //
    public hive_session( String h, String p, String n, String w )
    {
        auth = hive_parameter.value( "hive_auth" );

        if ( auth == null )
            auth = "userIdPassword";

        host = h;
        port = p;
        name = n;
        pass = w;
        kerb = "";
    }

    //
    public hive_session( String h, String p, String k )
    {
        auth = hive_parameter.value( "hive_auth" );

        if ( auth == null )
            auth = "userIdPassword";

        host = h;
        port = p;
        kerb = k;
        name = "";
        pass = "";
    }

    //
    public hive_session( oracle.sql.STRUCT obj )
        throws SQLException
    {
        if ( obj != null )
        {
            oracle.sql.Datum[] atr = obj.getOracleAttributes();

            auth = hive_parameter.value( "hive_auth" );

            if ( auth == null )
                auth = "userIdPassword";

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

            if ( atr.length == 4 )
            {
                if ( atr[ 2 ] != null )
                    name = atr[ 2 ].toString();
                else
                    name = "";

                if ( atr[ 3 ] != null )
                    pass = atr[ 3 ].toString();
                else
                    pass = "";

                kerb = "";
            }
            else if ( atr.length == 3 )
            {
                if ( atr[ 2 ] != null )
                    kerb = atr[ 2 ].toString();
                else
                    kerb = "";

                name = "";
                pass = "";
            }
            else
            {
                name = "";
                pass = "";
                kerb = "";
            }
        }
    }

    //
    public String toString()
    {
        String str = "";

        str += "host: " + host + "\n";
        str += "port: " + port + "\n";

        str += "auth: " + auth + "\n";

        if ( auth == "userIdPassword" )
        {
            str += "name: " + name + "\n";
            str += "pass: " + pass + "\n";
        }
        else if ( auth == "kerberos" )
        {
            str += "kerb: " + kerb + "\n";
        }
        else
        {
            // nothing to do ?
        }

        return str;
    }

    //
    public boolean equals( hive_session val )
    {
        boolean eq = false;

        if ( val != null )
        {
            if ( auth.equals( val.auth )
              && host.equals( val.host )
              && port.equals( val.port )
              && name.equals( val.name )
              && pass.equals( val.pass )
              && kerb.equals( val.kerb ) )
                eq = true;
        }

        return eq;
    }
};

//
public class hive_bind
{
    //
    public static final int UNKNOWN        =  0;
    //
    public static final int SCOPE_IN       =  1;
    public static final int SCOPE_OUT      =  2;
    public static final int SCOPE_INOUT    =  3;
    //
    public static final int TYPE_BOOL      =  1;
    public static final int TYPE_DATE      =  2;
    public static final int TYPE_FLOAT     =  3;
    public static final int TYPE_INT       =  4;
    public static final int TYPE_LONG      =  5;
    public static final int TYPE_NULL      =  6;
    public static final int TYPE_ROWID     =  7;
    public static final int TYPE_SHORT     =  8;
    public static final int TYPE_STRING    =  9;
    public static final int TYPE_TIME      = 10;
    public static final int TYPE_TIMESTAMP = 11;
    public static final int TYPE_URL       = 12;

    //
    public String value;
    public int   type;
    public int   scope;

    //
    public hive_bind()
    {
        value = "";
        type  = UNKNOWN;
        scope = UNKNOWN;
    }

    //
    public hive_bind( String v, int t, int s )
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
                    type = atr[ 1 ].intValue();
                else
                    type = UNKNOWN;
            }
            else
                type = UNKNOWN;

            if ( atr.length > 2 )
            {
                if ( atr[ 2 ] != null )
                    scope = atr[ 2 ].intValue();
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

    //
    public boolean equals( hive_bind val )
    {
        boolean eq = false;

        if ( val != null )
        {
            if ( value.equals( val.value )
              && type == val.type
              && scope == val.scope )
                eq = true;
        }

        return eq;
    }

    //
    public boolean toBool()
    {
        boolean val = false;

        if ( value != null )
        {
            if ( value.equalsIgnoreCase( "Y" )
              || value.equalsIgnoreCase( "T" )
              || value.equalsIgnoreCase( "YES" )
              || value.equalsIgnoreCase( "TRUE" ) )
                val = true;
        }

        return val;
    }

    //
    public Date toDate()
    {
        Date val = null;

        if ( value != null )
        {
            try
            {
                String par = hive_parameter.value( "date_format" );

                if ( par == null )
                    par = new String( "YYYY-MM-DD" );

                DateFormat fmt = new SimpleDateFormat( par, Locale.ENGLISH );
                val = (java.sql.Date) fmt.parse( value );
            }
            catch ( Exception /*ParseException*/ ex ) {}
        }

        return val;
    }

    //
    public float toFloat()
    {
        float val = 0;

        if ( value != null )
        {
            val = Float.valueOf( value );
        }

        return val;
    }

    //
    public int toInt()
    {
        int val = 0;

        if ( value != null )
        {
            val = Integer.valueOf( value );
        }

        return val;
    }

    //
    public long toLong()
    {
        long val = 0;

        if ( value != null )
        {
            val = Long.valueOf( value );
        }

        return val;
    }

    //
    public String toRowid()
    {
        return toVarchar();
    }

    //
    public short toShort()
    {
        short val = 0;

        if ( value != null )
        {
            val = Short.valueOf( value );
        }

        return val;
    }

    //
    public String toVarchar()
    {
        return value;
    }

    //
    public Time toTime()
    {
        Time val = null;

        if ( value != null )
        {
            try
            {
                String par = hive_parameter.value( "time_format" );

                if ( par == null )
                    par = new String( "hh:mm a" );

                DateFormat fmt = new SimpleDateFormat( par, Locale.ENGLISH );
                val = new Time( ( (Date) fmt.parse( value ) ).getTime() );
            }
            catch ( ParseException ex ) {}
        }

        return val;
    }

    //
    public Timestamp toTimestamp()
    {
        Timestamp val = null;

        if ( value != null )
        {
            try
            {
                String par = hive_parameter.value( "timestamp_format" );

                if ( par == null )
                    par = new String( "yyyy-MM-dd hh:mm:ss.SSS" );

                DateFormat fmt = new SimpleDateFormat( par, Locale.ENGLISH );
                val = new Timestamp( ( (Date) fmt.parse( value ) ).getTime() );
            }
            catch ( ParseException ex ) {}
        }

        return val;
    }

    //
    public URL toUrl()
    {
        URL val = null;

        try
        {
            val = new URL( value );
        }
        catch ( MalformedURLException ex ) {}

        return val;
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
    public long size()
    {
        return binds.size();
    }

    //
    public String toString()
    {
        String str = "";

        for ( hive_bind bnd : binds )
            str += bnd.toString();

        return str;
    }

    //
    public boolean equals( hive_bindings val )
    {
        boolean eq = false;

        if ( val != null )
        {
            if ( val.binds != null )
            {
                if ( binds.size() == val.binds.size() )
                {
                    eq = true;

                    for ( int i = 0; i < binds.size(); i++ )
                    {
                        if ( ! binds.get( i ).equals( val.binds.get( i ) ) )
                        {
                            eq = false;
                            break;
                        }
                    }
                }
            }
        }

        return eq;
    }
};

// stored context records
public class hive_connection
{
    //
    private static String driver_;
    String url_;

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
    public hive_connection( oracle.sql.STRUCT obj )
        throws SQLException
    {
        session = new hive_session( obj );
    }

    //
    public hive_connection( String host, String port )
    {
        session = new hive_session( host, port );
    }

    //
    public hive_connection( String host, String port, String name, String pass )
    {
        session = new hive_session( host, port, name, pass );
    }

    //
    public hive_connection( String host, String port, String kerb )
    {
        session = new hive_session( host, name, kerb );
    }

    //
    static public void loadDriver() throws hive_exception
    {
        try
        {
            Class.forName( getDriverName() );
        }
        catch ( ClassNotFoundException e )
        {
            throw new hive_exception( "Driver class not foound: " + getDriverName() );
        }
    }

    //
    static public String getDriverName()
        throws hive_exception
    {
        if ( driver_ == null )
        {
            driver_ = hive_parameter.value( "hive_jdbc_driver" );

            if ( driver_ == null )
                throw new hive_exception( "Could not find parameter value for JDBC driver" );
        }

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
    public void setKerb( String val ) { session.kerb = val; }

    //
    public String getHost() { return session.host; }
    public String getPort() { return session.port; }
    public String getUser() { return session.name; }
    public String getPass() { return session.pass; }
    public String getKerb() { return session.kerb; }

    //
    public String getUrl() throws hive_exception
    {
        url_ = hive_parameter.value( "hive_jdbc_url" );

        if ( url_ == null )
            throw new hive_exception( "Could not find parameter for Hive URL" );

        if ( ( session.host.length() > 0 )
          && ( session.port.length() > 0 ) )
        {
            url_ += session.host + ":" + session.port;

            if ( session.auth != null )
            {
                url_ += ";AuthenticationMethod=" + session.auth;

                if ( session.auth == "userIdPassword" )
                {
                    if ( session.name != null )
                    {
                        if ( session.name.length() > 0 )
                            url_ += ";User=" + session.name;
                        else
                            throw new hive_exception( "Missing user in connection data" );
                    }
                    else
                        throw new hive_exception( "Encountered NULL user in connection data" );

                    if ( session.pass != null )
                    {
                        if ( session.pass.length() > 0 )
                            url_ +=";Password=" + session.pass;
                        else
                            throw new hive_exception( "Missing password in connection data" );
                    }
                    else
                        throw new hive_exception( "Encountered NULL password in connection data" );
                }
                else if ( session.auth == "kerberos" )
                {
                    if ( session.kerb != null )
                    {
                        if ( session.kerb.length() > 0 )
                            url_ +=";ServicePrincipalName=" + session.kerb;
                        else
                            throw new hive_exception( "Missing Kerberos principal in connection data" );
                    }
                    else
                        throw new hive_exception( "Encountered NULL Kerberos principal in connection data" );
                }
                else
                {
                    throw new hive_exception( "Unknown authentication method [" + session.auth + "] encountered" );
                }
            else
            {
                throw new hive_exception( "Encountered NULL session authentication method" );
            }
        }
        else
        {
            if ( session.host.length() == 0 )
                throw new hive_exception( "Missing host in connection data" );

            if ( session.port.length() == 0 )
                throw new hive_exception( "Missing port in connection data" );
        }
        
        log.info( url_ );
        return url_;
    }

    //
    public boolean loadConnection()
    {
        if ( session.host.length() == 0 )
            session.host = hive_parameter.value( "hive_host" );

        if ( session.port.length() == 0 )
            session.port = hive_parameter.value( "hive_port" );

        if ( session.auth == 'userIdPassword' )
        {
            if ( session.name.length() == 0 )
                session.name = hive_parameter.value( "hive_user" );

            if ( session.pass.length() == 0 )
                session.pass = hive_parameter.value( "hive_pass" );

            session.kerb = "";
        }
        else if ( session.auth == 'kerberos' )
        {
            if ( session.kerb.length() == 0 )
                session.kerb = hive_parameter.value( "hive_principal" );

            session.name = "";
            session.pass = "";
        }
        else
        {
            session.name = "";
            session.pass = "";
            session.kerb = "";
        }

        return ( ( session.host.length() > 0 )
              && ( session.port.length() > 0 )
              && ( ( session.kerb.length() > 0 )
                || ( ( session.name.length() > 0 )
                  && ( session.pass.length() > 0 ) ) ) );
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
                {
                    if ( session.auth == "kerberos" )
                        login();

                    conn_ = DriverManager.getConnection( url );
                }
            }
            else
                throw new hive_exception( "Could not load connection data" );
        }

        return conn_;
    }

    //
    public boolean login() throws SQLException, hive_exception
    {
        boolean ok = false;

        //
        String rlm = hive_parameter.value( "java.security.krb5.realm" );
        String kdc = hive_parameter.value( "java.security.krb5.kdc" );
        String cnf = hive_parameter.value( "java.security.krb5.conf" );
        String jdb = hive_parameter.value( "java.security.auth.login.config" );

        if ( rlm != null )
            System.setProperty( "java.security.krb5.realm", rlm );

        if ( kdc != null )
            System.setProperty( "java.security.krb5.kdc" );

        if ( cnf != null )
        {
            if ( cnf.indexOf( "?" ) >= 0 )
            {
                String oh = hive_parameter.env( "ORACLE_HOME" );

                if ( oh != null )
                    cnf = cnf.replace( "?", oh );
            }

            System.setProperty( "java.security.krb5.conf", cnf );
        }

        if ( jdb != null )
        {
            if ( jdb.indexOf( "?" ) >= 0 )
            {
                String oh = hive_parameter.env( "ORACLE_HOME" );

                if ( oh != null )
                    jdb = jdb.replace( "?", oh );
            }

            System.setProperty( "java.security.auth.login.config", jdb );
        }

        try
        {
            Subject sub = new Subject();
            LoginContext lc = new LoginContext( "JDBC_DRIVER_01", sub, new callback_handler() );

            lc.login();
            ok = true;
        }
        catch ( LoginException ex )
        {
            ok = false;
            throw new hive_exception( "Kerberos exception: " + ex.getMessage() );
        }

        return ok;
    }

    //
    public boolean equals( hive_connection val )
    {
        boolean eq = false;

        if ( val != null )
            eq = session.equals( val.session );

        return eq;
    }
};

//
public class hive_context
{
    //
    private hive_connection   con_;
    private hive_bindings     bnd_;

    //
    private String            sql_;
    private PreparedStatement stm_;
    private ResultSet         rst_;
    private ResultSetMetaData rmd_;

    //
    private long rec_;

    // ctor
    //
    public hive_context( String sql, oracle.sql.ARRAY bnd, oracle.sql.STRUCT con ) throws SQLException, hive_exception
    {
        log.trace( "hive_context ctor: " + sql );
        sql_ = sql;

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( bnd != null )
            bnd_ = new hive_bindings( bnd );
        else
            bnd_ = new hive_bindings();

        if ( con != null )
            con_ = new hive_connection( con );
        else
            con_ = new hive_connection();

        con_.loadDriver();
        con_.createConnection();

        rec_ = 0;
    }

    //
    public boolean ready()
    {
        log.trace( "hive_context ready: " + ( ! ( rst_ == null ) ) );
        return ( ! ( rst_ == null ) );
    }

    //
    public void clear()
    {
        log.trace( "hive_context clear" );
        con_ = null;
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
    public PreparedStatement applyBindings( PreparedStatement stmt )
        throws SQLException, hive_exception
    {
        if ( bnd_ != null )
        {
            if ( bnd_.size() > 0 )
            {
                int idx = 0;

                for ( hive_bind bnd : bnd_.binds )
                {
                    idx += 1;

                    switch ( bnd.type )
                    {
                        //
                        case hive_bind.TYPE_BOOL:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setBoolean( idx, bnd.toBool() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.NUMBER );

                            break;

                        //
                        case hive_bind.TYPE_DATE:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setDate( idx, bnd.toDate() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.DATE );

                            break;

                        //
                        case hive_bind.TYPE_FLOAT:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setFloat( idx, bnd.toFloat() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.NUMBER );

                            break;

                        //
                        case hive_bind.TYPE_INT:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setInt( idx, bnd.toInt() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.NUMBER );

                            break;

                        //
                        case hive_bind.TYPE_LONG:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setLong( idx, bnd.toLong() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.NUMBER );

                            break;

                        //
                        case hive_bind.TYPE_NULL:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setNull( idx, Types.VARCHAR );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Encountered OUT or INOUT scope for TYPE_NULL" );

                            break;

                        //
                        case hive_bind.TYPE_ROWID:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setString( idx, bnd.toRowid() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.VARCHAR );

                            break;

                        //
                        case hive_bind.TYPE_SHORT:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setShort( idx, bnd.toShort() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.NUMBER );

                            break;

                        //
                        case hive_bind.TYPE_STRING:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setString( idx, bnd.toVarchar() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.VARCHAR );

                            break;

                        //
                        case hive_bind.TYPE_TIME:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setTime( idx, bnd.toTime() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.DATE );

                            break;

                        //
                        case hive_bind.TYPE_TIMESTAMP:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setTimestamp( idx, bnd.toTimestamp() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.TIMESTAMP );

                            break;

                        //
                        case hive_bind.TYPE_URL:

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setURL( idx, bnd.toUrl() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.VARCHAR );

                            break;

                        //
                        default:
                            throw new hive_exception( "Unknown binding type [" + bnd.type + "] encountered" );
                    }
                }
            }
        }

        return stmt;
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

        log.trace( "hive_context columnCount rmd_: " + rmd_ );
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

        log.trace( "hive_context columnType rmd_: " + rmd_ );
        return rmd_.getColumnType( i );
    }

    // recordset
    //
    public boolean next() throws SQLException
    {
        ++rec_;

        log.trace( "hive_context next rst_: " + rst_ );
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
        log.trace( "hive_context execute" );

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
            PreparedStatement stm = applyBindings( con_.getConnection().prepareStatement( limitSql( sql_ ) ) );
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
            stm_ = applyBindings( con_.getConnection().prepareStatement( sql_ ) );

        log.trace( "hive_context setPreparedStatement returns: " + ( ! ( stm_ == null ) ) );
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

        log.trace( "hive_context setResultSet returns: " + ( ! ( rst_ == null ) ) );
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

        log.trace( "hive_context setResultSetMetaData returns: " + ( ! ( rmd_ == null ) ) );
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

    //
    public boolean equals( hive_context val )
    {
        boolean eq = false;

        if ( val != null )
        {
            if ( ( con_ != null )
              && ( bnd_ != null )
              && ( sql_ != null ) )
            {
                eq = ( ( con_.equals( val.con_ ) )
                    && ( bnd_.equals( val.bnd_ ) )
                    && ( sql_.equals( val.sql_ ) ) );
            }
            else
            {
                eq = true;

                if ( con_ != null )
                    eq = ( con_.equals( val.con_ ) && eq );

                if ( bnd_ != null )
                    eq = ( bnd_.equals( val.bnd_ ) && eq );

                if ( sql_ != null )
                    eq = ( sql_.equals( val.sql_ ) && eq );
            }
        }

        return eq;
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

            if ( itm.equals( ctx ) )
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

            log.trace( "hive_manager new map size: " + map_.size() );
        }
        else
        {
            log.trace( "hive_manager found existing context" );
        }

        log.trace( "hive_manager createContext return: " + key );
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
        log.trace( "getSQLTypeName called" );
        return sql_;
    }

    // override (SQLData inheritence)
    public void readSQL( SQLInput stream, String type )
        throws SQLException 
    {
        log.trace( "readSQL called" );
        sql_ = type;
        key_ = stream.readBigDecimal();
    }

    // override (SQLData inheritence)
    public void writeSQL( SQLOutput stream )
        throws SQLException 
    {
        log.trace( "writeSQL called" );
        stream.writeBigDecimal( key_ );
    }

    //
    static public BigDecimal SqlDesc( oracle.sql.ARRAY[] attr,  // out
                                      String             stmt,  // on
                                      oracle.sql.ARRAY   bnds, 
                                      oracle.sql.STRUCT  conn )
        throws SQLException, hive_exception
    {
        log.trace( "SqlDesc called" );

        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = new hive_context( stmt, bnds, conn );

        if ( ctx == null )
            throw new hive_exception( "Context not created for SqlDesc" );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        ResultSetMetaData rmd = ctx.descSql();
        log.trace( "SqlDesc: columns: " + rmd.getColumnCount() );

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
    static public BigDecimal SqlDesc( oracle.sql.ARRAY[] attr,  // out
                                      BigDecimal         key )  // in
        throws SQLException, hive_exception
    {
        log.trace( "SqlDesc called" );

        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = manager_.getContext( key );

        if ( ctx == null )
            throw new hive_exception( "Context not found for SqlDesc" );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        ResultSetMetaData rmd = ctx.descSql();
        log.trace( "SqlDesc: columns: " + rmd.getColumnCount() );

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
    static public BigDecimal SqlOpen( String            stmt,
                                      oracle.sql.ARRAY  bnds,
                                      oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        log.trace( "SqlOpen called [String]: " + stmt );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            log.trace( "SqlOpen created hive_manager" );
        }

        hive_context ctx = new hive_context( stmt, bnds, conn );
        key_ = manager_.createContext( ctx );

        log.trace( "SqlOpen returning key: " + key_ );

        return key_;
    }

    //
    static public BigDecimal SqlOpen( STRUCT[]          sctx,
                                      String            stmt,
                                      oracle.sql.ARRAY  bnds,
                                      oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        log.trace( "SqlOpen called [STRUCT]: " + sctx );
        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        //
        key_ = SqlOpen( stmt, bnds, conn );

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
    static public BigDecimal SqlOpen( BigDecimal[]      key,
                                      String            stmt,
                                      oracle.sql.ARRAY  bnds,
                                      oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        log.trace( "SqlOpen called" );

        key_ = SqlOpen( stmt, bnds, conn );

        if ( key_.intValue() == 0 )
            return FAILURE;

        key[ 0 ] = key_;

        return SUCCESS;
    }

    static public BigDecimal SqlFetch( ARRAY[]    out,
                                       BigDecimal key, 
                                       BigDecimal num )
        throws SQLException, InvalidKeyException, hive_exception
    {
        log.trace( "SqlFetch called: key_ = " + key );

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
        log.trace( "SqlClose called" );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            log.trace( "SqlClose created hive_manager" );
        }

        hive_context ctx = manager_.removeContext( key );

        if ( ctx != null )
            ctx.clear();

        return SUCCESS;
    }
};
/

--
set define on

--
show errors

--
-- ... done!
--
