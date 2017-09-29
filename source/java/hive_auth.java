/*
  Hive-ODCI - Copyright (C) 2006-2016 Metasystems Technologies Inc. (MTI)
  Nicholas Van Wyen
  
  This library is free software; you can redistribute it and/or modify it 
  under the terms of the GNU Lesser General Public License as published by 
  the Free Software Foundation; either version 2.1 of the License, or (at 
  your option) any later version.
  
  This library is distributed in the hope that it will be useful, but 
  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
  or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
  License for more details.
  
  You should have received a copy of the GNU Lesser General Public License 
  along with this library; if not, write to the
  
                  Free Software Foundation, Inc.
                  59 Temple Place, Suite 330,
                  Boston, MA 02111-1307 USA
*/


package oracle.mti.odci;

//
import java.io.*;
import java.net.*;
import java.sql.*;
import java.lang.*;
import java.util.*;

import oracle.sql.*;
import oracle.jdbc.*;

//
@SuppressWarnings("deprecation")
public class hive_auth
{
    private static String grant_; // parameters: auth_no_grant_action
    private static String table_; //             auth_no_table_defined
    private static String auto_;  //             auth_auto_grant

    //
    public static final String[] OPERATIONS = {
                                                "SELECT",       // DML
                                                "LOAD",
                                                "INSERT",
                                                "UPDATE",
                                                "DELETE",
                                                "MERGE",
                                                "SHOW",
                                                "DESCRIBE",
                                                "CREATE",       // DDL
                                                "DROP",
                                                "TRUNCATE",
                                                "ALTER"
                                              };

    public static final int ACTION_GRANT  = 0;
    public static final int ACTION_REVOKE = 1;

    // ctor
    public hive_auth()
    {
        grant_ = hive_parameter.value( "auth_no_grant_action" );
        table_ = hive_parameter.value( "auth_table_undefined" );
        auto_  = hive_parameter.value( "auth_auto_grant" );

        // default as needed ...
        if ( ( grant_ == null ) || ( grant_.length() == 0 ) )
            grant_ = "ignore";

        if ( ( table_ == null ) || ( table_.length() == 0 ) )
            table_ = "allow";

        if ( ( auto_ == null ) || ( auto_.length() == 0 ) )
            auto_ = "";
    }

    //
    static public void grant( String table, String grantee, String operation )
        throws SQLException, hive_exception
    {
        //
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            String[] gnt = grantee.trim().split( "," );
            String[] opr = operation.trim().split( "," );

            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            if ( con.getAutoCommit() )
                con.setAutoCommit( false );

            //
            for ( String act : gnt )
            {
                act = act.trim().toUpperCase();

                //
                for ( String prm : opr )
                {
                    prm = prm.trim().toUpperCase();

                    if ( is_operation( prm ) )
                    {
                        if ( ! is_granted( table, act, prm ) )
                        {
                            String sql = "insert into auth$ ( tab, id#, opr ) " +
                                         "select ?, id#, ? from ora$user$ where name = ?";

                            stm = con.prepareStatement( sql );

                            stm.setString( 1, table );
                            stm.setString( 2, prm );
                            stm.setString( 3, act );

                            stm.executeUpdate();
                            stm.close();

                        }
                    }
                }
            }

            //
            con.commit();

            stm.close();
        }
        catch ( SQLException ex )
        {
            con.rollback();
            log.error( "hive_auth::grant SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            con.rollback();
            log.error( "hive_auth::grant Exception: " + log.stack( ex ) );
            throw ex;
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
    static public void revoke( String table, String grantee, String operation )
        throws SQLException, hive_exception
    {
        //
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            String[] gnt = grantee.trim().split( "," );
            String[] opr = operation.trim().split( "," );

            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            if ( con.getAutoCommit() )
                con.setAutoCommit( false );

            //
            for ( String act : gnt )
            {
                act = act.trim().toUpperCase();

                //
                for ( String prm : opr )
                {
                    prm = prm.trim().toUpperCase();

                    if ( is_operation( prm ) )
                    {
                        if ( is_granted( table, act, prm ) )
                        {
                            String sql = "delete from auth$ " +
                                          "where tab = ? " +
                                            "and opr = ? " +
                                            "and id# = " +
                                              "( select id# " +
                                                  "from ora$user$ " +
                                                 "where name = ? )";

                            stm = con.prepareStatement( sql );

                            stm.setString( 1, table );
                            stm.setString( 2, prm );
                            stm.setString( 3, act );

                            stm.executeUpdate();
                            stm.close();

                        }
                    }
                }
            }


            //
            con.commit();

            stm.close();
        }
        catch ( SQLException ex )
        {
            con.rollback();
            log.error( "hive_auth::revoke SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            con.rollback();
            log.error( "hive_auth::revoke Exception: " + log.stack( ex ) );
            throw ex;
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
    static public boolean has_grants( String table )
        throws SQLException, hive_exception
    {
        boolean ok = false;

        //
        Connection con = null;
        PreparedStatement stm = null;

        try
        {
            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            String sql = "select count(0) total from auth$ where tab = ?";

            //
            stm = con.prepareStatement( sql );
            stm.setString( 1, table );

            //
            ResultSet rst = stm.executeQuery();

            //
            if ( rst.next() )
                ok = ( rst.getLong( "total" ) > 0 );

            rst.close();
            stm.close();

            if ( ! ok )
            {
                if ( grant_.equalsIgnoreCase( "auto" ) )
                {
                    // auth_auto_grant
                    if ( ( auto_ != null ) && ( auto_.length() > 0 ) )
                    {
                        String[] lst = auto_.trim().split( "," );

                        if ( lst.length > 0 )
                        {
                            for ( String tok : lst )
                            {
                                if ( tok.trim().equalsIgnoreCase( "current" ) )
                                    grant( table, login_user(), "select" );
                                else
                                    grant( table, tok.trim(), "select" );
                            }

                            ok = true;
                        }
                    }
                }
                else if ( grant_.equalsIgnoreCase( "log" ) )
                {
                    log.info( "hive_auth::has_grants No grants defined for table [" + table + "]" );
                }
                else if ( grant_.equalsIgnoreCase( "error" ) )
                {
                    throw new hive_exception( "No grants defined for table [" + table + "]" );
                }
                else 
                {
                    // ... ignoring
                }
            }
        }
        catch ( SQLException ex )
        {
            log.error( "hive_auth::has_grants SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            log.error( "hive_auth::has_grants Exception: " + log.stack( ex ) );
            throw ex;
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();

                // *** do not close the "default" connection ***
            }
            catch ( SQLException ex )
            {
                // ... do nothing!
            }
        }

        return ok;
    } 

    //
    static public boolean permitted( String table, String grantee, String operation )
        throws SQLException, hive_exception
    {
        boolean ok = false;

        if ( has_grants( table ) )
        {
            ok = user_permitted( table, grantee, operation )
              || role_permitted( table, grantee, operation );  
        }
        else
            ok = true;      // no existing grants is equivalent to ALLOW ANY to ALL

        return ok;
    }

    //
    static public boolean permitted( String table, String operation )
        throws SQLException, hive_exception
    {
        return permitted( table, login_user(), operation );
    }

    //
    static public boolean permitted( String sql, ResultSetMetaData rmd )
        throws SQLException, hive_exception
    {
        boolean ok = false;

        if ( ( rmd != null ) && ( rmd.getColumnCount() > 0 ) )
        {
            String cmd = sql_operation( sql );
            String usr = login_user();

            for ( int i = 1; i <= rmd.getColumnCount(); ++ i )
            {
                String tab = rmd.getTableName( i );

                if ( ! permitted( tab, usr, cmd ) )
                    throw new hive_exception( "Operation [" + cmd + "] not permitted for table [" + tab + "]" );
            }

            ok = true;
        }
        else
            ok = true;

        return ok;
    }

    //
    static public boolean permitted( String sql )
        throws SQLException, hive_exception
    {
        boolean ok = false;

        try
        {
            List<String> lst = hive_parser.table_list( sql );

            if ( ( lst != null ) && ( lst.size() > 0 ) )
            {
                String cmd = sql_operation( sql );
                String usr = login_user();

                for ( String tab : lst )
                {
                    if ( ! permitted( tab, usr, cmd ) )
                        throw new hive_exception( "Operation [" + cmd + "] not permitted for table [" + tab + "]" );
                }

                ok = true;  // nothing denied
            }
            else
            {
                if ( table_.equalsIgnoreCase( "error" ) )
                    throw new hive_exception( "Undefined table data for SQL" );
                else if ( table_.equalsIgnoreCase( "deny" ) )
                    ok = false;
                else if ( table_.equalsIgnoreCase( "allow" ) )
                    ok = true;
                else
                    ok = true;  // default "allow"

            }
        }
        catch ( hive_exception ex )
        {
            log.error( "hive_auth::permitted error: " + log.stack( ex ) );
            throw ex;
        }

        return ok;
    }

    //
    static public boolean user_permitted( String table, String grantee, String operation )
        throws SQLException, hive_exception
    {
        boolean ok = false;

        //
        Connection con = null;
        PreparedStatement stm = null;        

        try
        {
            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            String sql = "select count(0) granted " +
                            "from auth$ a, " +
                                 "ora$user$ b " +
                           "where a.id# = b.id# " +
                             "and a.tab = ? " +
                             "and a.opr = ? " +
                             "and b.name = ? " +
                             "and b.type = 1";

            //
            stm = con.prepareStatement( sql );
            stm.setString( 1, table );
            stm.setString( 2, operation );
            stm.setString( 3, grantee );

            //
            ResultSet rst = stm.executeQuery();

            //
            if ( rst.next() )
                ok = ( rst.getLong( "granted" ) > 0 );

            rst.close();
            stm.close();
        }
        catch ( SQLException ex )
        {
            log.error( "hive_auth::user_permitted SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            log.error( "hive_auth::user_permitted Exception: " + log.stack( ex ) );
            throw ex;
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();

                // *** do not close the "default" connection ***
            }
            catch ( SQLException ex )
            {
                // ... do nothing!
            }
        }

        return ok;
    }

    //
    static public boolean role_permitted( String table, String grantee, String operation )
        throws SQLException, hive_exception
    {
        boolean ok = false;

        //
        Connection con = null;
        PreparedStatement stm = null;        

        try
        {
            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            String sql = "select count(0) granted " +
                           "from auth$ a " +
                          "where tab = ? " +
                            "and opr = ? " +
                            "and id# in ( select b.id# " +
                                           "from ora$user$ b, " +
                                                "( select /*+ connect_by_filtering */ " +
                                                         "grantee#, " +
                                                         "granted_role# " +
                                                    "from ora$role$priv$ " +
                                                 "connect by grantee# = prior granted_role# " +
                                                   "start with grantee = ? ) c " +
                                          "where b.id# = c.granted_role# " +
                                            "and b.type = 0 )";
            

            //
            stm = con.prepareStatement( sql );
            stm.setString( 1, table );
            stm.setString( 2, operation );
            stm.setString( 3, grantee );

            //
            ResultSet rst = stm.executeQuery();

            //
            if ( rst.next() )
                ok = ( rst.getLong( "granted" ) > 0 );

            rst.close();
            stm.close();
        }
        catch ( SQLException ex )
        {
            log.error( "hive_auth::role_permitted SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            log.error( "hive_auth::role_permitted Exception: " + log.stack( ex ) );
            throw ex;
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();

                // *** do not close the "default" connection ***
            }
            catch ( SQLException ex )
            {
                // ... do nothing!
            }
        }

        return ok;
    }

    //
    static public boolean is_granted( String table, String grantee, String operation )
        throws SQLException, hive_exception
    {
        boolean ok = false;

        //
        Connection con = null;
        PreparedStatement stm = null;        

        try
        {
            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            String sql = "select count(0) granted " +
                            "from auth$ a, " +
                                 "ora$user$ b " +
                           "where a.id# = b.id# " +
                             "and a.tab = ? " +
                             "and a.opr = ? " +
                             "and b.name = ?";
                           // no type specified (works with users & roles both)

            //
            stm = con.prepareStatement( sql );
            stm.setString( 1, table );
            stm.setString( 2, operation );
            stm.setString( 3, grantee );

            //
            ResultSet rst = stm.executeQuery();

            //
            if ( rst.next() )
                ok = ( rst.getLong( "granted" ) > 0 );

            rst.close();
            stm.close();
        }
        catch ( SQLException ex )
        {
            log.error( "hive_auth::is_granted SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            log.error( "hive_auth::is_granted Exception: " + log.stack( ex ) );
            throw ex;
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();

                // *** do not close the "default" connection ***
            }
            catch ( SQLException ex )
            {
                // ... do nothing!
            }
        }

        return ok;
    }

    //
    static public String login_user()
        throws SQLException, hive_exception
    {
        String usr = "";

        //
        Connection con = null;
        PreparedStatement stm = null;        

        try
        {
            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            String sql = "select user name from dual";

            //
            stm = con.prepareStatement( sql );
            ResultSet rst = stm.executeQuery();

            //
            if ( rst.next() )
                usr = rst.getString( "name" );

            rst.close();
            stm.close();
        }
        catch ( SQLException ex )
        {
            log.error( "hive_auth::login_user SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            log.error( "hive_auth::login_user Exception: " + log.stack( ex ) );
            throw ex;
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();

                // *** do not close the "default" connection ***
            }
            catch ( SQLException ex )
            {
                // ... do nothing!
            }
        }

        return usr;
    }

    //
    static public boolean is_operation( String operation )
    {
        boolean ok = false;

        for ( String opr : OPERATIONS )
        {
            if ( operation.equalsIgnoreCase( opr ) )
            {
                ok = true;
                break;
            }
        }

        return ok;
    }

    //
    static public String sql_operation( String sql )
    {
        String cmd = "";
        int typ = hive_parser.command_type( sql );

        if ( typ != hive_parser.COMMAND_UNKNOWN )
        {
            switch ( hive_parser.command_type( sql ) )
            {
                case hive_parser.COMMAND_SELECT:
                    cmd = "SELECT";
                    break;

                case hive_parser.COMMAND_DELETE:
                    cmd = "DELETE";
                    break;

                case hive_parser.COMMAND_INSERT:
                    cmd = "INSERT";
                    break;

                case hive_parser.COMMAND_UPDATE:
                    cmd = "UPDATE";
                    break;

                case hive_parser.COMMAND_REPLACE:
                    cmd = "REPLACE";
                    break;

                case hive_parser.COMMAND_MERGE:
                    cmd = "MERGE";
                    break;

                case hive_parser.COMMAND_CREATE:
                    cmd = "CREATE";
                    break;

                case hive_parser.COMMAND_INDEX:
                    cmd = "INDEX";
                    break;

                case hive_parser.COMMAND_VIEW:
                    cmd = "VIEW";
                    break;

                case hive_parser.COMMAND_ALTER:
                    cmd = "ALTER";
                    break;

                default:
                    break;
            }                    
        }
        else
        {
            // if the SQL Parser couldn't determine the "command type", 
            // then do a simple parse, just the first word
            sql = sql.replaceAll( "[\\t\\n\\r]+", " " ).trim();
            String[] tok = sql.split( " " );

            if ( tok.length > 0 )
                cmd = tok[ 0 ].toUpperCase();
        }

        return cmd.trim();
    }
};
