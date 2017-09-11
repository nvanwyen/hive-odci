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

import oracle.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.OracleTypes;

//
@SuppressWarnings("deprecation")
public class hive_types
{
    /*
        This java class duplicates the PL/SQL SYS.DBMS_TYPES
        package specification type codes for convenience
        as of 12c (may not be applicable in later version)
    */
    public static final int TYPECODE_DATE              =  12;
    public static final int TYPECODE_NUMBER            =   2;
    public static final int TYPECODE_RAW               =  95;
    public static final int TYPECODE_CHAR              =  96;
    public static final int TYPECODE_VARCHAR2          =   9;
    public static final int TYPECODE_VARCHAR           =   1;
    public static final int TYPECODE_MLSLABEL          = 105;
    public static final int TYPECODE_BLOB              = 113;
    public static final int TYPECODE_BFILE             = 114;
    public static final int TYPECODE_CLOB              = 112;
    public static final int TYPECODE_CFILE             = 115;
    public static final int TYPECODE_TIMESTAMP         = 187;
    public static final int TYPECODE_TIMESTAMP_TZ      = 188;
    public static final int TYPECODE_TIMESTAMP_LTZ     = 232;
    public static final int TYPECODE_INTERVAL_YM       = 189;
    public static final int TYPECODE_INTERVAL_DS       = 190;

    public static final int TYPECODE_REF               = 110;
    public static final int TYPECODE_OBJECT            = 108;
    public static final int TYPECODE_VARRAY            = 247;            /* COLLECTION TYPE */
    public static final int TYPECODE_TABLE             = 248;            /* COLLECTION TYPE */
    public static final int TYPECODE_NAMEDCOLLECTION   = 122;
    public static final int TYPECODE_OPAQUE            =  58;            /* OPAQUE TYPE */

    /* 
        These typecodes are for use in AnyData api only and are short forms
        for the corresponding char typecodes with a charset form of SQLCS_NCHAR.
    */
    public static final int TYPECODE_NCHAR             = 286;
    public static final int TYPECODE_NVARCHAR2         = 287;
    public static final int TYPECODE_NCLOB             = 288;

    /* Typecodes for Binary Float, Binary Double and Urowid. */
    public static final int TYPECODE_BFLOAT            = 100;
    public static final int TYPECODE_BDOUBLE           = 101;
    public static final int TYPECODE_UROWID            = 104;

    /*
        Java SQL type support
    */
    public static final int TYPEJDBC_ARRAY            = java.sql.Types.ARRAY;
    public static final int TYPEJDBC_BIGINT           = java.sql.Types.BIGINT;
    public static final int TYPEJDBC_BINARY           = java.sql.Types.BINARY;
    public static final int TYPEJDBC_BIT              = java.sql.Types.BIT;
    public static final int TYPEJDBC_BLOB             = java.sql.Types.BLOB;
    public static final int TYPEJDBC_BOOLEAN          = java.sql.Types.BOOLEAN;
    public static final int TYPEJDBC_CHAR             = java.sql.Types.CHAR;
    public static final int TYPEJDBC_CLOB             = java.sql.Types.CLOB;
    public static final int TYPEJDBC_DATALINK         = java.sql.Types.DATALINK;
    public static final int TYPEJDBC_DATE             = java.sql.Types.DATE;
    public static final int TYPEJDBC_DECIMAL          = java.sql.Types.DECIMAL;
    public static final int TYPEJDBC_DISTINCT         = java.sql.Types.DISTINCT;
    public static final int TYPEJDBC_DOUBLE           = java.sql.Types.DOUBLE;
    public static final int TYPEJDBC_FLOAT            = java.sql.Types.FLOAT;
    public static final int TYPEJDBC_INTEGER          = java.sql.Types.INTEGER;
    public static final int TYPEJDBC_JAVA_OBJECT      = java.sql.Types.JAVA_OBJECT;
    public static final int TYPEJDBC_LONGNVARCHAR     = java.sql.Types.LONGNVARCHAR;
    public static final int TYPEJDBC_LONGVARBINARY    = java.sql.Types.LONGVARBINARY;
    public static final int TYPEJDBC_LONGVARCHAR      = java.sql.Types.LONGVARCHAR;
    public static final int TYPEJDBC_NCHAR            = java.sql.Types.NCHAR;
    public static final int TYPEJDBC_NCLOB            = java.sql.Types.NCLOB;
    public static final int TYPEJDBC_NULL             = java.sql.Types.NULL;
    public static final int TYPEJDBC_NUMERIC          = java.sql.Types.NUMERIC;
    public static final int TYPEJDBC_NVARCHAR         = java.sql.Types.NVARCHAR;
    public static final int TYPEJDBC_OTHER            = java.sql.Types.OTHER;
    public static final int TYPEJDBC_REAL             = java.sql.Types.REAL;
    public static final int TYPEJDBC_REF              = java.sql.Types.REF;
    public static final int TYPEJDBC_ROWID            = java.sql.Types.ROWID;
    public static final int TYPEJDBC_SMALLINT         = java.sql.Types.SMALLINT;
    public static final int TYPEJDBC_SQLXML           = java.sql.Types.SQLXML;
    public static final int TYPEJDBC_STRUCT           = java.sql.Types.STRUCT;
    public static final int TYPEJDBC_TIME             = java.sql.Types.TIME;
    public static final int TYPEJDBC_TIMESTAMP        = java.sql.Types.TIMESTAMP;
    public static final int TYPEJDBC_TINYINT          = java.sql.Types.TINYINT;
    public static final int TYPEJDBC_VARBINARY        = java.sql.Types.VARBINARY;
    public static final int TYPEJDBC_VARCHAR          = java.sql.Types.VARCHAR;

    /*
        ODCI return 
    */
    public static final int SUCCESS                    =   0;
    public static final int NO_DATA                    = 100;

    //
    static public int to_jdbc_type( int typ )
    {
        int ret = 0;

        //
        switch ( typ )
        {
            case TYPECODE_DATE:
                ret = TYPEJDBC_DATE;
                break;

            case TYPECODE_NUMBER:
                ret = TYPEJDBC_NUMERIC;
                break;

            case TYPECODE_RAW:
                ret = TYPEJDBC_LONGVARBINARY;
                break;

            case TYPECODE_CHAR:
                ret = TYPEJDBC_CHAR;
                break;

            case TYPECODE_VARCHAR2:
            case TYPECODE_VARCHAR:
                ret = TYPEJDBC_VARCHAR;
                break;

            case TYPECODE_MLSLABEL:
                ret = TYPEJDBC_STRUCT;
                break;

            case TYPECODE_BLOB:
                ret = TYPEJDBC_BLOB;
                break;

            case TYPECODE_BFILE:
                ret = TYPEJDBC_OTHER;
                break;

            case TYPECODE_CLOB:
                ret = TYPEJDBC_CLOB;
                break;

            case TYPECODE_CFILE:
                ret = TYPEJDBC_OTHER;
                break;

            case TYPECODE_TIMESTAMP:
            case TYPECODE_TIMESTAMP_TZ:
            case TYPECODE_TIMESTAMP_LTZ:
                ret = TYPEJDBC_TIMESTAMP;
                break;

            case TYPECODE_INTERVAL_YM:
            case TYPECODE_INTERVAL_DS:
                ret = TYPEJDBC_TIMESTAMP;
                break;

            case TYPECODE_REF:
                ret = TYPEJDBC_REF;
                break;

            case TYPECODE_OBJECT:
                ret = TYPEJDBC_JAVA_OBJECT;
                break;

            case TYPECODE_VARRAY:
                ret = TYPEJDBC_ARRAY;
                break;

            case TYPECODE_TABLE:
                ret = TYPEJDBC_STRUCT;
                break;

            case TYPECODE_NAMEDCOLLECTION:
                ret = TYPEJDBC_STRUCT;
                break;

            case TYPECODE_OPAQUE:
                ret = TYPEJDBC_JAVA_OBJECT;
                break;

            case TYPECODE_NCHAR:
                ret = TYPEJDBC_CHAR;
                break;

            case TYPECODE_NVARCHAR2:
                ret = TYPEJDBC_VARCHAR;
                break;

            case TYPECODE_NCLOB:
                ret = TYPEJDBC_BLOB;
                break;

            case TYPECODE_BFLOAT:
                ret = TYPEJDBC_FLOAT;
                break;

            case TYPECODE_BDOUBLE:
                ret = TYPEJDBC_DOUBLE;
                break;

            case TYPECODE_UROWID:
                ret = TYPEJDBC_ROWID;
                break;

            default:
                ret = TYPEJDBC_NULL;
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
            case TYPEJDBC_VARCHAR:
            case TYPEJDBC_CHAR:
            case TYPEJDBC_NVARCHAR:
            case TYPEJDBC_NCHAR:
                ret = TYPECODE_VARCHAR2;
                break;

            case TYPEJDBC_BIGINT:
            case TYPEJDBC_DOUBLE:
            case TYPEJDBC_FLOAT:
            case TYPEJDBC_INTEGER:
            case TYPEJDBC_NUMERIC:
            case TYPEJDBC_REAL:
            case TYPEJDBC_SMALLINT:
            case TYPEJDBC_TINYINT:
            case TYPEJDBC_DECIMAL:
            case TYPEJDBC_BOOLEAN:
                ret = TYPECODE_NUMBER;
                break;

            case TYPEJDBC_CLOB:
            case TYPEJDBC_NCLOB:
                ret = TYPECODE_CLOB;
                break;

            case TYPEJDBC_BLOB:
                ret = TYPECODE_BLOB;
                break;

            case TYPEJDBC_DATE:
                ret = TYPECODE_DATE;
                break;

            case TYPEJDBC_TIME:
            case TYPEJDBC_TIMESTAMP:
                ret = TYPECODE_TIMESTAMP;
                break;

            case TYPEJDBC_LONGNVARCHAR:
            case TYPEJDBC_LONGVARBINARY:
            case TYPEJDBC_LONGVARCHAR:
            case TYPEJDBC_ARRAY:
            case TYPEJDBC_BINARY:
            case TYPEJDBC_BIT:
            case TYPEJDBC_DATALINK:
            case TYPEJDBC_DISTINCT:
            case TYPEJDBC_JAVA_OBJECT:
            case TYPEJDBC_NULL:
            case TYPEJDBC_OTHER:
            case TYPEJDBC_REF:
            case TYPEJDBC_ROWID:
            case TYPEJDBC_SQLXML:
            case TYPEJDBC_STRUCT:
            case TYPEJDBC_VARBINARY:
            default:
                ret = typ; // ? unknown ?
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
            log.error( "hive_types::nls_charset_id SQLException: " + log.stack( ex ) + log.code( ex ) );
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive_types::nls_charset_id Exception: " + log.stack( ex ) );
        }
        finally
        {
            try
            {
                //
                if ( stm != null )
                    stm.close();
            }
            catch ( SQLException ex ) 
            {
                log.error( "hive_types::nls_charset_id (finally_block) SQLException: " + log.stack( ex ) + log.code( ex ) );
            }
            catch ( Exception ex )
            {
                log.error( "hive_types::nls_charset_id (finally_block) Exception: " + log.stack( ex ) );
            }

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

    //
    public static int to_typecode( String str )
    {
        int typ = 0;

        if ( str.trim().equalsIgnoreCase( "DATE" ) )
            typ = TYPECODE_DATE;
        else if ( str.trim().equalsIgnoreCase( "NUMBER" ) )
            typ = TYPECODE_NUMBER;
        else if ( str.trim().equalsIgnoreCase( "RAW" ) )
            typ = TYPECODE_RAW;
        else if ( str.trim().equalsIgnoreCase( "CHAR" ) )
            typ = TYPECODE_CHAR;
        else if ( str.trim().equalsIgnoreCase( "VARCHAR2" ) )
            typ = TYPECODE_VARCHAR2;
        else if ( str.trim().equalsIgnoreCase( "VARCHAR" ) )
            typ = TYPECODE_VARCHAR;
        else if ( str.trim().equalsIgnoreCase( "MLSLABEL" ) )
            typ = TYPECODE_MLSLABEL;
        else if ( str.trim().equalsIgnoreCase( "BLOB" ) )
            typ = TYPECODE_BLOB;
        else if ( str.trim().equalsIgnoreCase( "BFILE" ) )
            typ = TYPECODE_BFILE;
        else if ( str.trim().equalsIgnoreCase( "CLOB" ) )
            typ = TYPECODE_CLOB;
        else if ( str.trim().equalsIgnoreCase( "CFILE" ) )
            typ = TYPECODE_CFILE;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMP" ) )
            typ = TYPECODE_TIMESTAMP;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMP_TZ" ) )
            typ = TYPECODE_TIMESTAMP_TZ;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMP_LTZ" ) )
            typ = TYPECODE_TIMESTAMP_LTZ;
        else if ( str.trim().equalsIgnoreCase( "INTERVAL_YM" ) )
            typ = TYPECODE_INTERVAL_YM;
        else if ( str.trim().equalsIgnoreCase( "INTERVAL_DS" ) )
            typ = TYPECODE_INTERVAL_DS;
        else if ( str.trim().equalsIgnoreCase( "REF" ) )
            typ = TYPECODE_REF;
        else if ( str.trim().equalsIgnoreCase( "OBJECT" ) )
            typ = TYPECODE_OBJECT;
        else if ( str.trim().equalsIgnoreCase( "VARRAY" ) )
            typ = TYPECODE_VARRAY;
        else if ( str.trim().equalsIgnoreCase( "TABLE" ) )
            typ = TYPECODE_TABLE;
        else if ( str.trim().equalsIgnoreCase( "NAMEDCOLLECTION" ) )
            typ = TYPECODE_NAMEDCOLLECTION;
        else if ( str.trim().equalsIgnoreCase( "OPAQUE" ) )
            typ = TYPECODE_OPAQUE;
        else if ( str.trim().equalsIgnoreCase( "NCHAR" ) )
            typ = TYPECODE_NCHAR;
        else if ( str.trim().equalsIgnoreCase( "NVARCHAR2" ) )
            typ = TYPECODE_NVARCHAR2;
        else if ( str.trim().equalsIgnoreCase( "NCLOB" ) )
            typ = TYPECODE_NCLOB;
        else if ( str.trim().equalsIgnoreCase( "BFLOAT" ) )
            typ = TYPECODE_BFLOAT;
        else if ( str.trim().equalsIgnoreCase( "BDOUBLE" ) )
            typ = TYPECODE_BDOUBLE;
        else if ( str.trim().equalsIgnoreCase( "UROWID" ) )
            typ = TYPECODE_UROWID;
        else 
            typ = 0;

        return typ;
    }

    //
    public static int to_typejdbc( String str )
    {
        int typ = 0;

        if ( str.trim().equalsIgnoreCase( "ARRAY" ) )
            typ = TYPEJDBC_ARRAY;
        else if ( str.trim().equalsIgnoreCase( "BFILE" ) )
            typ = TYPEJDBC_BLOB;
        else if ( str.trim().equalsIgnoreCase( "BIGINT" ) )
            typ = TYPEJDBC_BIGINT;
        else if ( str.trim().equalsIgnoreCase( "BINARY" ) )
            typ = TYPEJDBC_BINARY;
        else if ( str.trim().equalsIgnoreCase( "BINARY_DOUBLE" ) )
            typ = TYPEJDBC_DOUBLE;
        else if ( str.trim().equalsIgnoreCase( "BINARY_FLOAT" ) )
            typ = TYPEJDBC_FLOAT;
        else if ( str.trim().equalsIgnoreCase( "BIT" ) )
            typ = TYPEJDBC_BIT;
        else if ( str.trim().equalsIgnoreCase( "BLOB" ) )
            typ = TYPEJDBC_BLOB;
        else if ( str.trim().equalsIgnoreCase( "BOOLEAN" ) )
            typ = TYPEJDBC_BOOLEAN;
        else if ( str.trim().equalsIgnoreCase( "CHAR" ) )
            typ = TYPEJDBC_CHAR;
        else if ( str.trim().equalsIgnoreCase( "CLOB" ) )
            typ = TYPEJDBC_CLOB;
        else if ( str.trim().equalsIgnoreCase( "CURSOR" ) )
            typ = TYPEJDBC_STRUCT;
        else if ( str.trim().equalsIgnoreCase( "DATALINK" ) )
            typ = TYPEJDBC_DATALINK;
        else if ( str.trim().equalsIgnoreCase( "DATE" ) )
            typ = TYPEJDBC_DATE;
        else if ( str.trim().equalsIgnoreCase( "DECIMAL" ) )
            typ = TYPEJDBC_DECIMAL;
        else if ( str.trim().equalsIgnoreCase( "DOUBLE" ) )
            typ = TYPEJDBC_DOUBLE;
        else if ( str.trim().equalsIgnoreCase( "FIXED_CHAR" ) )
            typ = TYPEJDBC_CHAR;
        else if ( str.trim().equalsIgnoreCase( "FLOAT" ) )
            typ = TYPEJDBC_FLOAT;
        else if ( str.trim().equalsIgnoreCase( "INTEGER" ) )
            typ = TYPEJDBC_INTEGER;
        else if ( str.trim().equalsIgnoreCase( "INTERVAL_DS" ) )
            typ = TYPEJDBC_TIME;
        else if ( str.trim().equalsIgnoreCase( "INTERVAL_YM" ) )
            typ = TYPEJDBC_TIME;
        else if ( str.trim().equalsIgnoreCase( "JAVA_OBJECT" ) )
            typ = TYPEJDBC_OTHER;
        else if ( str.trim().equalsIgnoreCase( "LONGVARBINARY" ) )
            typ = TYPEJDBC_LONGVARBINARY;
        else if ( str.trim().equalsIgnoreCase( "LONGVARCHAR" ) )
            typ = TYPEJDBC_LONGVARCHAR;
        else if ( str.trim().equalsIgnoreCase( "NULL" ) )
            typ = TYPEJDBC_NULL;
        else if ( str.trim().equalsIgnoreCase( "NUMBER" ) )
            typ = TYPEJDBC_NUMERIC;
        else if ( str.trim().equalsIgnoreCase( "NUMERIC" ) )
            typ = TYPEJDBC_NUMERIC;
        else if ( str.trim().equalsIgnoreCase( "OPAQUE" ) )
            typ = TYPEJDBC_OTHER;
        else if ( str.trim().equalsIgnoreCase( "OTHER" ) )
            typ = TYPEJDBC_OTHER;
        else if ( str.trim().equalsIgnoreCase( "PLSQL_INDEX_TABLE" ) )
            typ = TYPEJDBC_STRUCT;
        else if ( str.trim().equalsIgnoreCase( "RAW" ) )
            typ = TYPEJDBC_BLOB;
        else if ( str.trim().equalsIgnoreCase( "REAL" ) )
            typ = TYPEJDBC_REAL;
        else if ( str.trim().equalsIgnoreCase( "REF" ) )
            typ = TYPEJDBC_REF;
        else if ( str.trim().equalsIgnoreCase( "ROWID" ) )
            typ = TYPEJDBC_ROWID;
        else if ( str.trim().equalsIgnoreCase( "SMALLINT" ) )
            typ = TYPEJDBC_SMALLINT;
        else if ( str.trim().equalsIgnoreCase( "STRUCT" ) )
            typ = TYPEJDBC_STRUCT;
        else if ( str.trim().equalsIgnoreCase( "TIME" ) )
            typ = TYPEJDBC_TIME;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMP" ) )
            typ = TYPEJDBC_TIMESTAMP;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMPLTZ" ) )
            typ = TYPEJDBC_TIMESTAMP;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMPNS" ) )
            typ = TYPEJDBC_TIMESTAMP;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMPTZ" ) )
            typ = TYPEJDBC_TIMESTAMP;
        else if ( str.trim().equalsIgnoreCase( "TINYINT" ) )
            typ = TYPEJDBC_TINYINT;
        else if ( str.trim().equalsIgnoreCase( "VARBINARY" ) )
            typ = TYPEJDBC_VARBINARY;
        else if ( str.trim().equalsIgnoreCase( "VARCHAR" ) )
            typ = TYPEJDBC_VARCHAR;
        else if ( str.trim().equalsIgnoreCase( "VARCHAR2" ) )
            typ = TYPEJDBC_VARCHAR;
        else 
            typ = 0;

        return typ;
    }

    //
    public static String to_typecode( int typ )
    {
        String val = "";

        switch ( typ )
        {
            case TYPECODE_BDOUBLE:
                val = "TYPECODE_BDOUBLE";
                break;

            case TYPECODE_BFLOAT:
                val = "TYPECODE_BFLOAT";
                break;

            case TYPECODE_NUMBER:            
                val = "TYPECODE_NUMBER";
                break;

            case TYPECODE_TIMESTAMP:
                val = "TYPECODE_TIMESTAMP";
                break;

            case TYPECODE_TIMESTAMP_LTZ:
                val = "TYPECODE_TIMESTAMP_LTZ";
                break;

            case TYPECODE_TIMESTAMP_TZ:
                val = "TYPECODE_TIMESTAMP_TZ";
                break;

            case TYPECODE_BFILE:
                val = "TYPECODE_BFILE";
                break;

            case TYPECODE_BLOB:
                val = "TYPECODE_BLOB";
                break;

            case TYPECODE_CFILE:
                val = "TYPECODE_CFILE";
                break;

            case TYPECODE_CHAR:
                val = "TYPECODE_CHAR";
                break;

            case TYPECODE_CLOB:
                val = "TYPECODE_CLOB";
                break;

            case TYPECODE_DATE:
                val = "TYPECODE_DATE";
                break;

            case TYPECODE_INTERVAL_DS:
                val = "TYPECODE_INTERVAL_DS";
                break;

            case TYPECODE_INTERVAL_YM:
                val = "TYPECODE_INTERVAL_YM";
                break;

            case TYPECODE_MLSLABEL:
                val = "TYPECODE_MLSLABEL";
                break;

            case TYPECODE_NAMEDCOLLECTION:
                val = "TYPECODE_NAMEDCOLLECTION";
                break;

            case TYPECODE_NCHAR:
                val = "TYPECODE_NCHAR";
                break;

            case TYPECODE_NCLOB:
                val = "TYPECODE_NCLOB";
                break;

            case TYPECODE_NVARCHAR2:
                val = "TYPECODE_NVARCHAR2";
                break;

            case TYPECODE_OBJECT:
                val = "TYPECODE_OBJECT";
                break;

            case TYPECODE_OPAQUE:
                val = "TYPECODE_OPAQUE";
                break;

            case TYPECODE_RAW:
                val = "TYPECODE_RAW";
                break;

            case TYPECODE_REF:
                val = "TYPECODE_REF";
                break;

            case TYPECODE_TABLE:
                val = "TYPECODE_TABLE";
                break;

            case TYPECODE_UROWID:
                val = "TYPECODE_UROWID";
                break;

            case TYPECODE_VARCHAR:
                val = "TYPECODE_VARCHAR";
                break;

            case TYPECODE_VARCHAR2:
                val = "TYPECODE_VARCHAR2";
                break;

            case TYPECODE_VARRAY:
                val = "TYPECODE_VARRAY";
                break;

            default:
                break;
        }

        return val;
    }

    //
    public static String to_typejdbc( int typ )
    {
        String val = "";

        switch ( typ )
        {
            case TYPEJDBC_ARRAY:
                val = "TYPEJDBC_ARRAY";
                break;

            case TYPEJDBC_BIGINT:
                val = "TYPEJDBC_BIGINT";
                break;

            case TYPEJDBC_BINARY:
                val = "TYPEJDBC_BINARY";
                break;

            case TYPEJDBC_BIT:
                val = "TYPEJDBC_BIT";
                break;

            case TYPEJDBC_BLOB:
                val = "TYPEJDBC_BLOB";
                break;

            case TYPEJDBC_BOOLEAN:
                val = "TYPEJDBC_BOOLEAN";
                break;

            case TYPEJDBC_CHAR:
                val = "TYPEJDBC_CHAR";
                break;

            case TYPEJDBC_CLOB:
                val = "TYPEJDBC_CLOB";
                break;

            case TYPEJDBC_DATALINK:
                val = "TYPEJDBC_DATALINK";
                break;

            case TYPEJDBC_DATE:
                val = "TYPEJDBC_DATE";
                break;

            case TYPEJDBC_DECIMAL:
                val = "TYPEJDBC_DECIMAL";
                break;

            case TYPEJDBC_DISTINCT:
                val = "TYPEJDBC_DISTINCT";
                break;

            case TYPEJDBC_DOUBLE:
                val = "TYPEJDBC_DOUBLE";
                break;

            case TYPEJDBC_FLOAT:
                val = "TYPEJDBC_FLOAT";
                break;

            case TYPEJDBC_INTEGER:
                val = "TYPEJDBC_INTEGER";
                break;

            case TYPEJDBC_JAVA_OBJECT:
                val = "TYPEJDBC_JAVA_OBJECT";
                break;

            case TYPEJDBC_LONGNVARCHAR:
                val = "TYPEJDBC_LONGNVARCHAR";
                break;

            case TYPEJDBC_LONGVARBINARY:
                val = "TYPEJDBC_LONGVARBINARY";
                break;

            case TYPEJDBC_LONGVARCHAR:
                val = "TYPEJDBC_LONGVARCHAR";
                break;

            case TYPEJDBC_NCHAR:
                val = "TYPEJDBC_NCHAR";
                break;

            case TYPEJDBC_NCLOB:
                val = "TYPEJDBC_NCLOB";
                break;

            case TYPEJDBC_NULL:
                val = "TYPEJDBC_NULL";
                break;

            case TYPEJDBC_NUMERIC:
                val = "TYPEJDBC_NUMERIC";
                break;

            case TYPEJDBC_NVARCHAR:
                val = "TYPEJDBC_NVARCHAR";
                break;

            case TYPEJDBC_OTHER:
                val = "TYPEJDBC_OTHER";
                break;

            case TYPEJDBC_REAL:
                val = "TYPEJDBC_REAL";
                break;

            case TYPEJDBC_REF:
                val = "TYPEJDBC_REF";
                break;

            case TYPEJDBC_ROWID:
                val = "TYPEJDBC_ROWID";
                break;

            case TYPEJDBC_SMALLINT:
                val = "TYPEJDBC_SMALLINT";
                break;

            case TYPEJDBC_SQLXML:
                val = "TYPEJDBC_SQLXML";
                break;

            case TYPEJDBC_STRUCT:
                val = "TYPEJDBC_STRUCT";
                break;

            case TYPEJDBC_TIME:
                val = "TYPEJDBC_TIME";
                break;

            case TYPEJDBC_TIMESTAMP:
                val = "TYPEJDBC_TIMESTAMP";
                break;

            case TYPEJDBC_TINYINT:
                val = "TYPEJDBC_TINYINT";
                break;

            case TYPEJDBC_VARBINARY:
                val = "TYPEJDBC_VARBINARY";
                break;

            case TYPEJDBC_VARCHAR:
                val = "TYPEJDBC_VARCHAR";
                break;

            default:
                break;
        }

        return val;
    }

    //
    public static int default_precision_typecode( int typ )
    {
        int def = 0;

        switch ( typ )
        {
            case TYPECODE_CHAR:
            case TYPECODE_VARCHAR2:
            case TYPECODE_VARCHAR:
            case TYPECODE_VARRAY:
            case TYPECODE_TABLE:
            case TYPECODE_NAMEDCOLLECTION:
            case TYPECODE_NCHAR:
            case TYPECODE_NVARCHAR2:
                def = 1;
                break;

            case TYPECODE_BLOB:
            case TYPECODE_BFILE:
            case TYPECODE_CLOB:
            case TYPECODE_CFILE:
            case TYPECODE_REF:
            case TYPECODE_NCLOB:
                def = -1;
                break;

            case TYPECODE_INTERVAL_YM:
            case TYPECODE_INTERVAL_DS:
                def = 2;
                break;

            case TYPECODE_BDOUBLE:
            case TYPECODE_BFLOAT:
            case TYPECODE_DATE:
            case TYPECODE_MLSLABEL:
            case TYPECODE_NUMBER:
            case TYPECODE_OBJECT:
            case TYPECODE_OPAQUE:
            case TYPECODE_RAW:
            case TYPECODE_TIMESTAMP:
            case TYPECODE_TIMESTAMP_LTZ:
            case TYPECODE_TIMESTAMP_TZ:
            case TYPECODE_UROWID:
            default:
                def = 0;
                break;
        }

        return def;
    }

    //
    public static int default_precision_typejdbc( int typ )
    {
        int def = 0;

        switch ( typ )
        {
            case TYPEJDBC_ARRAY:
            case TYPEJDBC_CHAR:
            case TYPEJDBC_LONGVARBINARY:
            case TYPEJDBC_LONGVARCHAR:
            case TYPEJDBC_NCHAR:
            case TYPEJDBC_NVARCHAR:
            case TYPEJDBC_VARBINARY:
            case TYPEJDBC_VARCHAR:
                def = 1;
                break;

            case TYPEJDBC_BINARY:
            case TYPEJDBC_BLOB:
            case TYPEJDBC_CLOB:
            case TYPEJDBC_DATALINK:
            case TYPEJDBC_JAVA_OBJECT:
            case TYPEJDBC_OTHER:
            case TYPEJDBC_REF:
            case TYPEJDBC_SQLXML:
            case TYPEJDBC_STRUCT:
                def = -1;
                break;

            case TYPEJDBC_TIME:
            case TYPEJDBC_TIMESTAMP:
                def = 2;
                break;

            case TYPEJDBC_BIGINT:
            case TYPEJDBC_BIT:
            case TYPEJDBC_BOOLEAN:
            case TYPEJDBC_DATE:
            case TYPEJDBC_DECIMAL:
            case TYPEJDBC_DOUBLE:
            case TYPEJDBC_FLOAT:
            case TYPEJDBC_INTEGER:
            case TYPEJDBC_NULL:
            case TYPEJDBC_NUMERIC:
            case TYPEJDBC_REAL:
            case TYPEJDBC_ROWID:
            case TYPEJDBC_SMALLINT:
            case TYPEJDBC_TINYINT:
            default:
                def = 0;
                break;
        }

        return def;
    }

    //
    public static int default_scale_typecode( int typ )
    {
        int def = 0;

        switch ( typ )
        {
            case TYPECODE_BDOUBLE:
            case TYPECODE_BFLOAT:
            case TYPECODE_NUMBER:            
                def = -127;
                break;

            case TYPECODE_TIMESTAMP:
            case TYPECODE_TIMESTAMP_LTZ:
            case TYPECODE_TIMESTAMP_TZ:
                def = 6;
                break;

            case TYPECODE_BFILE:
            case TYPECODE_BLOB:
            case TYPECODE_CFILE:
            case TYPECODE_CHAR:
            case TYPECODE_CLOB:
            case TYPECODE_DATE:
            case TYPECODE_INTERVAL_DS:
            case TYPECODE_INTERVAL_YM:
            case TYPECODE_MLSLABEL:
            case TYPECODE_NAMEDCOLLECTION:
            case TYPECODE_NCHAR:
            case TYPECODE_NCLOB:
            case TYPECODE_NVARCHAR2:
            case TYPECODE_OBJECT:
            case TYPECODE_OPAQUE:
            case TYPECODE_RAW:
            case TYPECODE_REF:
            case TYPECODE_TABLE:
            case TYPECODE_UROWID:
            case TYPECODE_VARCHAR:
            case TYPECODE_VARCHAR2:
            case TYPECODE_VARRAY:
            default:
                def = 0;
                break;
        }

        return def;
    }

    //
    public static int default_scale_typejdbc( int typ )
    {
        int def = 0;

        switch ( typ )
        {
            case TYPEJDBC_BIGINT:
            case TYPEJDBC_BINARY:
            case TYPEJDBC_BIT:
            case TYPEJDBC_BOOLEAN:
            case TYPEJDBC_DECIMAL:
            case TYPEJDBC_DOUBLE:
            case TYPEJDBC_FLOAT:
            case TYPEJDBC_INTEGER:
            case TYPEJDBC_NUMERIC:
            case TYPEJDBC_SMALLINT:
            case TYPEJDBC_TINYINT:
                def = -127;
                break;

            case TYPEJDBC_TIME:
            case TYPEJDBC_TIMESTAMP:
                def = 6;
                break;

            case TYPEJDBC_ARRAY:
            case TYPEJDBC_BLOB:
            case TYPEJDBC_CHAR:
            case TYPEJDBC_CLOB:
            case TYPEJDBC_DATALINK:
            case TYPEJDBC_DATE:
            case TYPEJDBC_DISTINCT:
            case TYPEJDBC_JAVA_OBJECT:
            case TYPEJDBC_LONGNVARCHAR:
            case TYPEJDBC_LONGVARBINARY:
            case TYPEJDBC_LONGVARCHAR:
            case TYPEJDBC_NCHAR:
            case TYPEJDBC_NCLOB:
            case TYPEJDBC_NULL:
            case TYPEJDBC_NVARCHAR:
            case TYPEJDBC_OTHER:
            case TYPEJDBC_REAL:
            case TYPEJDBC_REF:
            case TYPEJDBC_ROWID:
            case TYPEJDBC_SQLXML:
            case TYPEJDBC_STRUCT:
            case TYPEJDBC_VARBINARY:
            case TYPEJDBC_VARCHAR:
            default:
                def = 0;
                break;
        }

        return def;
    }

    //
    static public Clob to_clob( String val ) throws SQLException, Exception
    {
        //
        Clob clob = null;
        Connection con = null;

        //
        try
        {
            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            clob = con.createClob();

            if ( ( val != null ) && ( val.length() > 0 ) )
                clob.setString( 1, val );
            else
                clob.setString( 1, "" );
        }
        catch ( SQLException ex )
        {
            //
            log.error( "hive_types::to_clob SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive_types::to_clob Exception: " + log.stack( ex ) );
            throw ex;
        }
        finally
        {
            // ... nothing to do
        }

        //
        return clob;
    }

    //
    static public Blob to_blob( Object val ) throws SQLException, Exception
    {
        //
        Blob blob = null;
        Connection con = null;

        //
        try
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream( out );
            os.writeObject( val );
            
            //
            con = DriverManager.getConnection( "jdbc:default:connection:" );

            //
            blob = con.createBlob();

            if ( val != null )
                blob.setBytes( 1, out.toByteArray() );
            else
                blob.setBytes( 1,  new byte[0] );
        }
        catch ( SQLException ex )
        {
            //
            log.error( "hive_types::to_blob SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive_types::to_blob Exception: " + log.stack( ex ) );
            throw ex;
        }
        finally
        {
            // ... nothing to do
        }

        //
        return blob;
    }
};
