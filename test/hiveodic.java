import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Properties;

import java.lang.IllegalArgumentException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.security.auth.*;
import javax.security.auth.login.*;
import javax.security.auth.callback.*;
import javax.security.auth.kerberos.*; 

//
class DefaultCallbackHandler implements CallbackHandler
{
    public void handle(Callback[] callbacks)
        throws IOException, UnsupportedCallbackException
    {
        System.out.println( "*** DefaultCallbackHandler::handle called" );

        for (int i = 0; i < callbacks.length; i++)
        {
            if ( callbacks[i] instanceof NameCallback )
            {
                System.out.println( "*** DefaultCallbackHandler::handle callbacks[" + i + "] instanceof NameCallback" );
                NameCallback nc = (NameCallback)callbacks[i];
                // nc.setName(username);
                nc.setName( "" );
            }
            else if ( callbacks[i] instanceof PasswordCallback )
            {
                System.out.println( "*** DefaultCallbackHandler::handle callbacks[" + i + "] instanceof PasswordCallback" );

                PasswordCallback pc = (PasswordCallback)callbacks[i];
                // pc.setPassword(password.toCharArray());
                pc.setPassword( ( new String( "" ) ).toCharArray() );
            }
            else
                throw new UnsupportedCallbackException( callbacks[i], "Unrecognised callback" );
        }
    }
}

public class hiveodic {
    //
    private static String driverName = "com.ddtek.jdbc.hive.HiveDriver";

    //
    private static String getProperty( String key )
    {
        String val = "";

        try
        {
            String cnf = "hiveodic.prop";
            Properties prop = new Properties();
            InputStream in;

            in = hiveodic.class.getClassLoader().getResourceAsStream( cnf );
 
            if ( in != null )
            {
                prop.load( in );
            }
            else
            {
                throw new FileNotFoundException( "property file '" + cnf + "' not found in the classpath" );
            }

            val = prop.getProperty( key );
        }
        catch ( FileNotFoundException ex )
        {
            ex.printStackTrace();
            System.exit(1);
        }
        catch ( IOException ex )
        {
            ex.printStackTrace();
            System.exit(1);
        }

        if ( val == null )
        {
            try
            {
                throw new IllegalArgumentException( "Missing property \"" + key + "\"" );
            }
            catch ( IllegalArgumentException ex )
            {
                ex.printStackTrace();
                System.exit(1);
            }
        }

        return val;
    }

    //
    private static Connection connect() throws SQLException
    {
        String url = getProperty( "url" );

        url = url.replace( "%host%", getProperty( "host" ) );
        url = url.replace( "%port%", getProperty( "port" ) );

        url += ";AuthenticationMethod=" + getProperty( "authmethod" );

        if ( getProperty( "authmethod" ).equals( "userIdPassword" ) )
            url += ";User=" + getProperty( "user" )  + ";Password=" + getProperty( "password" );
        else
        {
            LoginContext lc = null;

            try
            {
                System.out.println( "*** Creating connect LoginContext" );

                Subject sub = new Subject();
                lc = new LoginContext( "JDBC_DRIVER_01", sub, new DefaultCallbackHandler() );

                // attempt authentication
                System.out.println( "*** Calling lc.login()" );
                lc.login();

                System.out.println( "*** LoginContext: " + lc.toString() );
            }
            catch ( LoginException ex )
            {
                ex.printStackTrace();
                System.exit(1);
            }

            url += ";ServicePrincipalName=" + getProperty( "principal" );
        }

        return DriverManager.getConnection( url );
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
    private static void descSql( Connection con, String sql ) throws SQLException
    {
        PreparedStatement stm = con.prepareStatement( sql );
        ResultSet rst = stm.executeQuery();
        ResultSetMetaData rmd = rst.getMetaData();

        System.out.println( "Describing: " + sql );

        // type values: http://docs.oracle.com/javase/7/docs/api/constant-values.html#java.sql.Types

        if ( rmd.getColumnCount() > 0 )
        {
            System.out.format( "%-30s %-10s %5s %12s %12s %-8s\n", 
                               "Name", 
                               "Type",
                               "Code",
                               "Precision",
                               "Scale",
                               "Nullable" );
            System.out.format( "%30s %10s %5s %12s %12s %8s\n", 
                               "------------------------------", 
                               "----------", 
                               "-----", 
                               "------------", 
                               "------------", 
                               "----------" );

            for ( int i = 1; i <= rmd.getColumnCount(); ++i ) 
            {
                System.out.format( "%-30s %-10s %5d %12d %12d %-8s\n", 
                                   rmd.getColumnName(i), 
                                   rmd.getColumnTypeName(i),
                                   rmd.getColumnType(i),
                                   rmd.getPrecision(i),
                                   rmd.getScale(i),
                                   ( ( rmd.isNullable(i) == 0 ) ? "FALSE" : "TRUE" ) );
            }
        }

        rst.close();
        stm.close();

        System.out.println();
    }

    //
    private static void querySql( Connection con, String sql ) throws SQLException
    {
        int rows = 0;
        PreparedStatement stm = con.prepareStatement( sql );
        ResultSet rst = stm.executeQuery();
        ResultSetMetaData rmd = rst.getMetaData();

        System.out.println( "Executing: " + sql );

        if ( rmd.getColumnCount() > 0 )
        {
            while ( rst.next() )
            {
                rows++;

                for ( int i = 1; i <= rmd.getColumnCount(); ++i )
                {
                    if ( i > 1 )
                        System.out.print( "," );

                    int typ = rmd.getColumnType(i);

                    if ( typ == Types.VARCHAR || typ == Types.CHAR)
                    {
                        System.out.print( rst.getString(i) );
                    }
                    else
                    {
                        System.out.print( rst.getLong(i) );
                    }
                }

                System.out.println();
            }
        }

        stm.close();
        rst.close();

        System.out.println();
        System.out.println( rows + " returned" );
        System.out.println();
    }

    //
    public static int parse_args( String[] args )
    {
        int opt = 0;

        String act = getProperty( "action" );

        if ( act.equalsIgnoreCase( "describe" ) )
            opt |= 1;
        else if ( act.equalsIgnoreCase( "desc" ) )
            opt |= 1;
        else if ( act.equalsIgnoreCase( "query" ) )
            opt |= 2;
        else if ( act.equalsIgnoreCase( "both" ) )
            opt |= 3;
        else 
            throw new IllegalArgumentException( "Unknown argument [" + act + "]: " +
                                                "use describe|desc|query|both" );

        if ( opt == 0 )
            opt = 1;        // default "describe"

        return opt;
    }

    //
    public static void main(String[] args) throws SQLException {

        //
        int run = parse_args( args );

        try {
            Class.forName( getProperty( "driver" ) );
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("Using driver: " + driverName );

        Connection con = connect();
        String sql = getProperty( "sql" );

        if ( ( run & 1 ) == 1 )
            descSql( con, limitSql( sql ) );

        if ( ( run & 2 ) == 2 )
            querySql( con, sql );

        con.close();
    }
}
