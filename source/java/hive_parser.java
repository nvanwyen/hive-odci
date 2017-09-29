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
import java.lang.*;
import java.util.*;

import net.sf.jsqlparser.*;
import net.sf.jsqlparser.util.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.merge.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.statement.delete.*;
import net.sf.jsqlparser.statement.replace.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.create.index.*;
import net.sf.jsqlparser.statement.create.view.*;
import net.sf.jsqlparser.statement.alter.*;

// properties parsing class
@SuppressWarnings("deprecation")
public class hive_parser
{
    //
    public static final int COMMAND_UNKNOWN     = 0x0000;
    public static final int COMMAND_SELECT      = 0x0101;   // DML
    public static final int COMMAND_DELETE      = 0x0102;
    public static final int COMMAND_INSERT      = 0x0103;
    public static final int COMMAND_UPDATE      = 0x0104;
    public static final int COMMAND_REPLACE     = 0x0105;
    public static final int COMMAND_MERGE       = 0x0106;
    public static final int COMMAND_CREATE      = 0x0201;   // DDL
    public static final int COMMAND_INDEX       = 0x0202;
    public static final int COMMAND_VIEW        = 0x0203;
    public static final int COMMAND_ALTER       = 0x0204;

    //
    private static String par_;

    // ctor
    public hive_parser()
    {
        try
        {
            par_ = hive_parameter.value( "auth_sql_parse_error" );
        }
        catch ( Exception ex )
        {
            // on error, use default ...
        }

        // default as needed ...
        if ( ( par_ == null ) || ( par_.length() == 0 ) )
            par_ = "none";
    }

    //
    public static int command_type( Statement stm )
    {
        int typ = COMMAND_UNKNOWN;

        if ( stm != null )
        {
            if ( stm instanceof Select )
                typ = COMMAND_SELECT;
            else if ( stm instanceof Delete )
                typ = COMMAND_DELETE;
            else if ( stm instanceof Insert )
                typ = COMMAND_INSERT;
            else if ( stm instanceof Replace )
                typ = COMMAND_REPLACE;
            else if ( stm instanceof Update )
                typ = COMMAND_UPDATE;
            else if ( stm instanceof Merge )
                typ = COMMAND_MERGE;
            else if ( stm instanceof CreateTable )
                typ = COMMAND_CREATE;
            else if ( stm instanceof CreateIndex )
                typ = COMMAND_INDEX;
            else if ( stm instanceof CreateView )
                typ = COMMAND_VIEW;
            else if ( stm instanceof Alter )
                typ = COMMAND_ALTER;
            else
            {
                // unknown
            }
        }

        return typ;
    }

    //
    public static int command_type( String sql )
    {
        int typ = COMMAND_UNKNOWN;

        try
        {
            Statement stm = CCJSqlParserUtil.parse( sql );
            TablesNamesFinder fnd = new TablesNamesFinder();

            typ = command_type( stm );
        }
        catch ( JSQLParserException ex )
        {
            // ... nothing to do
        }
        catch ( Exception ex )
        {
            // ... nothing to do
        }

        return typ;
    }

    //
    public static List<String> table_list( String sql )
        throws hive_exception
    {
        List<String> lst = null;

        try
        {
            Statement stm = CCJSqlParserUtil.parse( sql );
            TablesNamesFinder fnd = new TablesNamesFinder();

            switch ( command_type( stm ) )
            {
                case COMMAND_SELECT:
                    lst = fnd.getTableList( (Select) stm );
                    break;

                case COMMAND_DELETE:
                    lst = fnd.getTableList( (Delete) stm );
                    break;

                case COMMAND_INSERT:
                    lst = fnd.getTableList( (Insert) stm );
                    break;

                case COMMAND_UPDATE:
                    lst = fnd.getTableList( (Update) stm );
                    break;

                case COMMAND_REPLACE:
                    lst = fnd.getTableList( (Replace) stm );
                    break;

                case COMMAND_MERGE:
                    lst = fnd.getTableList( (Merge) stm );
                    break;

                case COMMAND_CREATE:
                    lst = fnd.getTableList( (CreateTable) stm );
                    break;

                case COMMAND_INDEX:
                    lst = fnd.getTableList( (CreateIndex) stm );
                    break;

                case COMMAND_VIEW:
                    lst = fnd.getTableList( (CreateView) stm );
                    break;

                case COMMAND_ALTER:
                    lst = fnd.getTableList( (Alter) stm );
                    break;

                default:
                    break;
            }

            if ( lst == null )
                lst = new ArrayList<String>();
        }
        catch ( JSQLParserException ex )
        {
            if ( par_.equalsIgnoreCase( "error" ) )
            {
                log.error( "hive_parser::table_list Exception: " + log.stack( ex ) );
                throw new hive_exception( "Parse error: " + log.stack( ex ) );
            }
            else
                lst = null;    // none
        }
        catch ( Exception ex )
        {
            log.error( "hive_parser::table_list Exception: " + log.stack( ex ) );
            throw ex;
        }

        //
        return lst;
    }
};


