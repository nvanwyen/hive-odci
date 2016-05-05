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

import java.lang.IllegalArgumentException;

public class hiveodic {
    //
    private static String driverName = "com.ddtek.jdbc.hive.HiveDriver";


    //
    private static Connection connect( String usr, String pwd ) throws SQLException
    {
        String url = "jdbc:datadirect:hive://orabdc.local:10000;User=%u%;Password=%p%";

        url.replace( "%u%", usr );
        url.replace( "%p%", pwd );

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

        if ( rmd.getColumnCount() > 0 )
        {
            System.out.format( "%-20s %-10s %5s %12s %-8s\n", 
                               "Name", 
                               "Type",
                               "Code",
                               "Precision",
                               "Nullable" );
            System.out.format( "%-20s %-10s %12s %-8s\n", 
                               "--------------------", 
                               "----------",
                               "------------",
                               "------------",
                               "--------" );

            for ( int i = 1; i <= rmd.getColumnCount(); ++i ) 
            {
                System.out.format( "%-20s %-10s %5d %12d %-8s\n", 
                                   rmd.getColumnName(i), 
                                   rmd.getColumnTypeName(i),
                                   rmd.getColumnType(i),
                                   rmd.getPrecision(i),
                                   ( ( rmd.isNullable(i) == 0 ) ? "FALSE" : "TRUE" ) );
            }
        }

        rst.close();
        stm.close();
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
    }

    //
    public static int parse_args( String[] args )
    {
        int opt = 0;

        for ( int i = 0; i < args.length; ++i )
        {
            if ( args[i].equalsIgnoreCase( "describe" ) )
                opt |= 1;
            else if ( args[i].equalsIgnoreCase( "desc" ) )
                opt |= 1;
            else if ( args[i].equalsIgnoreCase( "-d" ) )
                opt |= 1;
            else if ( args[i].equalsIgnoreCase( "query" ) )
                opt |= 2;
            else if ( args[i].equalsIgnoreCase( "-q" ) )
                opt |= 2;
            else if ( args[i].equalsIgnoreCase( "both" ) )
                opt |= 3;
            else if ( args[i].equalsIgnoreCase( "-b" ) )
                opt |= 3;
            else 
                throw new IllegalArgumentException( "Unknown argument [" + args[i] + "]" );
        }

        if ( opt == 0 )
            opt = 1;        // default "describe"

        return opt;
    }

    //
    public static void main(String[] args) throws SQLException {
        //
        int run = parse_args( args );

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("Using driver: " + driverName );
        Connection con = connect( "oracle", "welcome1" );

        //String sql = "select * from movie where movie_id > 36000 and lower( title ) like '%here%' limit 10";
        //String sql = "select * from movie where movie_id > 0";
        String sql = "select * from cust";
        ////String sql = "select * from movie_view limit 10";
        //String sql = "select a.movie_id, a.title, b.avg_rating from movie a, movie_rating b where a.movie_id = b.movie_id and a.movie_id > 36000 and lower( a.title ) like '%here%' limit 10";
        //String sql = "select * from movie_view";
        //String sql = "select count(*) count_of from movie_view";

        if ( ( run & 1 ) == 1 )
            descSql( con, limitSql( sql ) );

        if ( ( run & 2 ) == 2 )
            querySql( con, sql );

        con.close();
    }
}
