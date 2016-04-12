--------------------------------------------------------------------------------
--
--
--


--
alter session set current_schema = hive;

--
create or replace and compile java source named "HiveJdbcClientExample" as

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.Driver;
import java.sql.DriverManager;

// ForName() class
import oracle.aurora.vm.OracleRuntime;
import oracle.aurora.rdbms.Schema;
import oracle.aurora.rdbms.DbmsJava;

//
public class ForName
{
    private Class from;

    //
    public ForName( Class from )
    {
        this.from = from;
    }

    //
    public ForName()
    {
        from = OracleRuntime.getCallerClass();
    }


    //
    public Class lookupWithClassLoader( String name ) throws ClassNotFoundException
    {
        //
        return Class.forName( name, true, from.getClassLoader() );
    }


    //
    static Class lookupWithSchema( String name, String schema ) throws ClassNotFoundException
    {
        Schema s = Schema.lookup(schema);
        return DbmsJava.classForNameAndSchema( name, s );
    }

    // //
    // static Class standardLoad( name ) throws ClassNotFoundException
    // {
    //     Class.forName( name );
    // }
}

//
public class HiveJdbcClientExample {

    //
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void run() throws SQLException {

        // option 1
        try {
            ForName fn = new ForName();
            fn.lookupWithClassLoader( driverName );
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        /*
        // option 2
        ForName.lookupWithSchema( driverName, "HIVE" );
        */

        /*
        // option 3
        try {
          Class.forName( driverName );
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        */

        Connection con = DriverManager.getConnection(
                "jdbc:hive2://orabdc.local:10000/default", "oracle", "welcome1");
        Statement stmt = con.createStatement();

        String tableName = "movie";
        ResultSet res;
        //stmt.executeQuery("drop table " + tableName);
        //ResultSet res = stmt.executeQuery("create table " + tableName
        //        + " (id int, name string, dept string)");

        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }

        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
          System.out.println(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(2));
        }
        
        // // load data into table
        // // NOTE: filepath has to be local to the hive server
        // // NOTE: /home/user/input.txt is a ctrl-A separated file with three fields per line
        // String filepath = "/home/user/input.txt";
        // sql = "load data local inpath '" + filepath + "' into table " + tableName;
        // System.out.println("Running: " + sql);
        // res = stmt.executeQuery(sql);
        
        // sql = "select * from empdata where id='1'";
        sql = "select * from movie where movie_id > 36000 and lower( title ) like '%here%'";

        res = stmt.executeQuery(sql);
        // show tables
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
            System.out.println(res.getString(2));
            System.out.println(res.getString(3));
        }
        res.close();
        stmt.close();
        con.close();
    }
}
/

show errors

create or replace procedure hive_test as
    language java name 'HiveJdbcClientExample.run()';
/

show errors
