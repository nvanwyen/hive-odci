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

// stored context records
public class hive_context
{
    ResultSet rset;
    String stmnt;
    public hive_context( String st, ResultSet rs ) { stmnt = st; rset = rs; }
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
            return "";

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
            ok = false;
        }
        catch ( Exception ex )
        {
            ok = false;
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
    public Connection createConnection() throws SQLException
    {
        if ( conn_ == null )
        {
            //
            if ( loadConnection() )
            {
                String url = getUrl();

                if ( url.length() > 0 )
                    conn_ = DriverManager.getConnection( url );
            }
        }

        return conn_;
    }

    //
    public ResultSet createResultSet( String stmnt ) throws SQLException
    {
        ResultSet rset;

        if ( createConnection() != null )
        {
            PreparedStatement stm = conn_.prepareStatement( limitSql( stmnt ) );
            rset = stm.executeQuery();
        }

        return rset;
    }

    //
    public static String limitSql( String sql )
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
    private hive_connection hive_;

    //
    private String sql_;        // SQL type name
    private BigDecimal key_;    // Context key

    //
    final static BigDecimal SUCCESS = new BigDecimal( 0 );
    final static BigDecimal FAILURE = new BigDecimal( 1 );

    //
    public hive()
    {
        hive_.loadDriver();
    }

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
