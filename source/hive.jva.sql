--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - hive.jva.sql
--

--
prompt ... running hive.jva.sql

--
alter session set current_schema = hive;

--
create or replace and compile java source "hive" as

//
package oracle.mti;

//
import java.io.*;
import java.util.*;
import java.sql.*;
import java.math.BigDecimal;
//
import oracle.sql.*;
import oracle.jdbc.*;
import oracle.CartridgeServices.*;

//
class hive_exception extends Exception
{
    public hive_exception( String msg )
    {
        super( msg );
    }
};

//
public class hive_connection
{
    //
    private static String driver_ = "com.ddtek.jdbc.hive.HiveDriver";
    String url_ = "jdbc:datadirect:hive://%h%:%p%;User=%u%;Password=%w%";

    //
    private String host_;
    private String port_;
    private String user_;
    private String pass_;

    //
    Connection conn_;

    //
    public hive_connection() {}

    //
    public hive_connection( String host, String port )
    {
        host_ = host;
        port_ = port;
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
    static public loadDriver() throws ClassNotFoundException
    {
        Class.forName( getDriverName() );
    }

    //
    static public getDriverName()
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
    public String getUrl()
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
                throw hive_exception( "Missing host in connection data" );

            if ( port_.length() == 0 )
                throw hive_exception( "Missing port in connection data" );

            if ( user_.length() == 0 )
                throw hive_exception( "Missing user in connection data" );

            if ( pass_.length() == 0 )
                throw hive_exception( "Missing password in connection data" );
        }

        return url_;
    }

    //
    public boolean loadConnection() throws SQLException
    {
        boolean ok = false;
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            String sql = "select name, " +
                                "value " +
                           "from param$ " +
                          "where name in ( 'default_hive_host', " +
                                          "'default_hive_port', " +
                                          "'default_hive_user', " +
                                          "'default_hive_pass' )";

            //
            con = new OracleDriver().defaultConnection();
            stm = con.prepareStatement( sql );

            ResultSet rst = stm.executeQuery();

            while ( rst.next() )
            {
                String name = rst.getString( "name" );
                String value = rst.getString( "value" );

                if ( name.length() > 0 )
                {
                    if ( name.trim().equalsIgnoreCase( "default_hive_host" ) )
                    {
                        if ( host_.length == 0 )
                            host_ = value;
                    }
                    else if ( name.trim().equalsIgnoreCase( "default_hive_port" ) )
                    {
                        if ( port_.length == 0 )
                            port_ = value;
                    }
                    else if ( name.trim().equalsIgnoreCase( "default_hive_user" ) )
                    {
                        if ( user_.length == 0 )
                            user_ = value;
                    }
                    else if ( name.trim().equalsIgnoreCase( "default_hive_pass" ) )
                    {
                        if ( pass_.length == 0 )
                            pass_ = value;
                    }
                }
            }

            //
            ok = true;
        }
        catch ( SQLException ex )
        {
            throw hive_exception( ex.getMessage() );
        }
        catch ( Exception ex )
        {
            throw hive_exception( ex.getMessage() );
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();
            }
            catch ( SQLException x ) {}
            catch ( Exception x )    {}

            // *** do not close the "default" connection ***
        }

        return ok;
    }

    //
    public Connection getConnection()
    {
        return conn_;
    }

    //
    public Connection createConnection() throws SQLException
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
                throw hive_exception( "Could not load connection data" );
        }

        return conn_;
    }
};

// stored context records
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
    public hive_context( String sql )
    {
        sql_ = sql;

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw hive_exception( "No SQL defined for hive context" );

        if ( hcn_ == null )
            hcn_ = new hive_connection();

        hcn_.loadDriver();
        hcn_.createConnection();
    }

    // members
    //
    public String getSql()                          { return sql_; }
    public PreparedStatement getPreparedStatement() { setPreparedStatement(); return stm_; }
    public ResultSet getResultSet()                 { setResultSet(); return rst_; }
    public ResultSetMetaData getResultSetMetaData() { setResultSetMetaData(); return rmd_; }

    // metadata
    //
    public int columnCount()                        { return rmd_.getColumnCount(); }
    public int columnType( int i )                  { return rmd_.getColumnType( i ); }

    // recordset
    //
    public boolean next()                           { return rst_.next(); }
    public int rowNumber()                          { return rst_.getRow(); }

    // data
    //
    public BigDecimal getBigDecimal( int i )        { return rst_.getBigDecimal( i ); }
    public BigDecimal getBigDecimal( String c )     { return rst_.getBigDecimal( c ); }

    //
    public boolean getBoolean( int i )              { return rst_.getBoolean( i ); }
    public boolean getBoolean( String c )           { return rst_.getBoolean( c ); }

    //
    public int getInt( int i )                      { return rst_.getInt( i ); }
    public int getInt( String c )                   { return rst_.getInt( c ); }

    //
    public long getLong( int i )                    { return rst_.getLong( i ); }
    public long getLong( String c )                 { return rst_.getLong( c ); }

    //
    public float getFloat( int i )                  { return rst_.getFloat( i ); }
    public float getFloat( String c )               { return rst_.getFloat( c ); }

    //
    public Date getDate( int i )                    { return rst_.getDate( i ); }
    public Date getDate( String c )                 { return rst_.getDate( c ); }

    //
    public String getString( int i )                { return rst_.getString( i ); }
    public String getString( String c )             { return rst_.getString( c ); }

    //
    public Timestamp getTimestamp( int i )          { return rst_.getTimestamp( i ); }
    public Timestamp getTimestamp( String c )       { return rst_.getTimestamp( c ); }

    //
    public ResultSetMetaData descSql()
    {
        ResultSetMetaData rmd;

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw hive_exception( "No SQL defined for hive context" );

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
    private boolean setPreparedStatement()
    {
        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw hive_exception( "No SQL defined for hive context" );

        if ( stm_ == null )
            stm_ = hcn_.getConnection().prepareStatement(sql_ );

        return ( ! ( stm_ == null ) );
    }

    //
    private boolean setResultSet()
    {
        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw hive_exception( "No SQL defined for hive context" );

        if ( rst_ == null )
        {
            if ( setPreparedStatement() )
                rst_ = stm_.executeQuery();
        }

        return ( ! ( rst_ == null ) );
    }

    //
    private boolean setResultSetMetaData()
    {
        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw hive_exception( "No SQL defined for hive context" );

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
    public String getSQLTypeName() throws SQLException 
    {
        return sql_;
    }

    // override (SQLData inheritence)
    public void readSQL( SQLInput stream, String type ) throws SQLException 
    {
        sql_ = type;
        key_ = stream.readBigDecimal();
    }

    // override (SQLData inheritence)
    public void writeSQL( SQLOutput stream ) throws SQLException 
    {
        stream.writeBigDecimal( key_ );
    }

    //
    static public BigDecimal ODCITableStart( STRUCT[] sctx, String stmt ) throws SQLException
    {
        // create context and result
        hive_context ctx = new hive_context( stmt, hive_.createResultSet( stmt ) );
        Connection con = DriverManager.getConnection("jdbc:default:connection:");

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

        StructDescriptor dsc = new StructDescriptor( "hive_t", con );
        sctx[ 0 ] = new STRUCT( dsc, con, imp );

        return SUCCESS;
    }


};
/

--
show errors

--
-- ... done!
--
