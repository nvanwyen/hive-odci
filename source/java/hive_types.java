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
    public static final int TYPECODE_OPAQUE            = 58;             /* OPAQUE TYPE */

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
        added support for oracle.jdbc.OracleTypes, which overlaps SYS.DBMS_TYPES
        constants used to identify SQL types, XOPEN equivalent
        ref: https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/oracle/jdbc/OracleTypes.html
    */
    public static final int TYPEJDBC_ARRAY             = OracleTypes.ARRAY;
    public static final int TYPEJDBC_BFILE             = OracleTypes.BFILE;
    public static final int TYPEJDBC_BIGINT            = OracleTypes.BIGINT;
    public static final int TYPEJDBC_BINARY            = OracleTypes.BINARY;
    public static final int TYPEJDBC_BINARY_DOUBLE     = OracleTypes.BINARY_DOUBLE;
    public static final int TYPEJDBC_BINARY_FLOAT      = OracleTypes.BINARY_FLOAT;
    public static final int TYPEJDBC_BIT               = OracleTypes.BIT;
    public static final int TYPEJDBC_BLOB              = OracleTypes.BLOB;
    public static final int TYPEJDBC_BOOLEAN           = OracleTypes.BOOLEAN;           // identifies generic SQL type BOOLEAN.
    public static final int TYPEJDBC_CHAR              = OracleTypes.CHAR;
    public static final int TYPEJDBC_CLOB              = OracleTypes.CLOB;
    public static final int TYPEJDBC_CURSOR            = OracleTypes.CURSOR;
    public static final int TYPEJDBC_DATALINK          = OracleTypes.DATALINK;          // identifies generic SQL type DATALINK
    public static final int TYPEJDBC_DATE              = OracleTypes.DATE;
    public static final int TYPEJDBC_DECIMAL           = OracleTypes.DECIMAL;
    public static final int TYPEJDBC_DOUBLE            = OracleTypes.DOUBLE;
    public static final int TYPEJDBC_FIXED_CHAR        = OracleTypes.FIXED_CHAR;        // Used when binding to CHAR in where clause
    public static final int TYPEJDBC_FLOAT             = OracleTypes.FLOAT;
    public static final int TYPEJDBC_INTEGER           = OracleTypes.INTEGER;
    public static final int TYPEJDBC_INTERVALDS        = OracleTypes.INTERVALDS;
    public static final int TYPEJDBC_INTERVALYM        = OracleTypes.INTERVALYM;
    public static final int TYPEJDBC_JAVA_OBJECT       = OracleTypes.JAVA_OBJECT;
    public static final int TYPEJDBC_JAVA_STRUCT       = OracleTypes.JAVA_STRUCT;
    public static final int TYPEJDBC_LONGVARBINARY     = OracleTypes.LONGVARBINARY;
    public static final int TYPEJDBC_LONGVARCHAR       = OracleTypes.LONGVARCHAR;
    public static final int TYPEJDBC_NULL              = OracleTypes.NULL;
    public static final int TYPEJDBC_NUMBER            = OracleTypes.NUMBER;            // shares value with NUMERIC as synonym
    public static final int TYPEJDBC_NUMERIC           = OracleTypes.NUMERIC;
    public static final int TYPEJDBC_OPAQUE            = OracleTypes.OPAQUE;
    public static final int TYPEJDBC_OTHER             = OracleTypes.OTHER;             // indicates SQL type for Java [get|set]Object
    public static final int TYPEJDBC_PLSQL_INDEX_TABLE = OracleTypes.PLSQL_INDEX_TABLE;
    public static final int TYPEJDBC_RAW               = OracleTypes.RAW;               // shares value with BINARY as synonym
    public static final int TYPEJDBC_REAL              = OracleTypes.REAL;
    public static final int TYPEJDBC_REF               = OracleTypes.REF;
    public static final int TYPEJDBC_ROWID             = OracleTypes.ROWID;
    public static final int TYPEJDBC_SMALLINT          = OracleTypes.SMALLINT;
    public static final int TYPEJDBC_STRUCT            = OracleTypes.STRUCT;
    public static final int TYPEJDBC_TIME              = OracleTypes.TIME;
    public static final int TYPEJDBC_TIMESTAMP         = OracleTypes.TIMESTAMP;
    public static final int TYPEJDBC_TIMESTAMPLTZ      = OracleTypes.TIMESTAMPLTZ;
    public static final int TYPEJDBC_TIMESTAMPNS       = OracleTypes.TIMESTAMPNS;       // Deprecated. since 9.2.0, use TIMESTAMP instead
    public static final int TYPEJDBC_TIMESTAMPTZ       = OracleTypes.TIMESTAMPTZ;
    public static final int TYPEJDBC_TINYINT           = OracleTypes.TINYINT;
 // public static final int TYPEJDBC_TRACE             = OracleTypes.TRACE;             // obsoleted, removed from oracle.jdbc.OracleTypes
    public static final int TYPEJDBC_VARBINARY         = OracleTypes.VARBINARY;
    public static final int TYPEJDBC_VARCHAR           = OracleTypes.VARCHAR;

    /*
        ODCI return 
    */
    public static final int SUCCESS                    = 0;
    public static final int NO_DATA                    = 100;

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
            log.error( "hive_types::nls_charset_id SQLException: " + ex.getMessage() );
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive_types::nls_charset_id Exception: " + ex.getMessage() );
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
                log.error( "hive_types::nls_charset_id (finally_block) SQLException: " + ex.getMessage() );
            }
            catch ( Exception ex )
            {
                log.error( "hive_types::nls_charset_id (finally_block) Exception: " + ex.getMessage() );
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
        if ( str.trim().equalsIgnoreCase( "DATE" ) )
            return TYPECODE_DATE;
        else if ( str.trim().equalsIgnoreCase( "NUMBER" ) )
            return TYPECODE_NUMBER;
        else if ( str.trim().equalsIgnoreCase( "RAW" ) )
            return TYPECODE_RAW;
        else if ( str.trim().equalsIgnoreCase( "CHAR" ) )
            return TYPECODE_CHAR;
        else if ( str.trim().equalsIgnoreCase( "VARCHAR2" ) )
            return TYPECODE_VARCHAR2;
        else if ( str.trim().equalsIgnoreCase( "VARCHAR" ) )
            return TYPECODE_VARCHAR;
        else if ( str.trim().equalsIgnoreCase( "MLSLABEL" ) )
            return TYPECODE_MLSLABEL;
        else if ( str.trim().equalsIgnoreCase( "BLOB" ) )
            return TYPECODE_BLOB;
        else if ( str.trim().equalsIgnoreCase( "BFILE" ) )
            return TYPECODE_BFILE;
        else if ( str.trim().equalsIgnoreCase( "CLOB" ) )
            return TYPECODE_CLOB;
        else if ( str.trim().equalsIgnoreCase( "CFILE" ) )
            return TYPECODE_CFILE;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMP" ) )
            return TYPECODE_TIMESTAMP;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMP_TZ" ) )
            return TYPECODE_TIMESTAMP_TZ;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMP_LTZ" ) )
            return TYPECODE_TIMESTAMP_LTZ;
        else if ( str.trim().equalsIgnoreCase( "INTERVAL_YM" ) )
            return TYPECODE_INTERVAL_YM;
        else if ( str.trim().equalsIgnoreCase( "INTERVAL_DS" ) )
            return TYPECODE_INTERVAL_DS;
        else if ( str.trim().equalsIgnoreCase( "REF" ) )
            return TYPECODE_REF;
        else if ( str.trim().equalsIgnoreCase( "OBJECT" ) )
            return TYPECODE_OBJECT;
        else if ( str.trim().equalsIgnoreCase( "VARRAY" ) )
            return TYPECODE_VARRAY;
        else if ( str.trim().equalsIgnoreCase( "TABLE" ) )
            return TYPECODE_TABLE;
        else if ( str.trim().equalsIgnoreCase( "NAMEDCOLLECTION" ) )
            return TYPECODE_NAMEDCOLLECTION;
        else if ( str.trim().equalsIgnoreCase( "OPAQUE" ) )
            return TYPECODE_OPAQUE;
        else if ( str.trim().equalsIgnoreCase( "NCHAR" ) )
            return TYPECODE_NCHAR;
        else if ( str.trim().equalsIgnoreCase( "NVARCHAR2" ) )
            return TYPECODE_NVARCHAR2;
        else if ( str.trim().equalsIgnoreCase( "NCLOB" ) )
            return TYPECODE_NCLOB;
        else if ( str.trim().equalsIgnoreCase( "BFLOAT" ) )
            return TYPECODE_BFLOAT;
        else if ( str.trim().equalsIgnoreCase( "BDOUBLE" ) )
            return TYPECODE_BDOUBLE;
        else if ( str.trim().equalsIgnoreCase( "UROWID" ) )
            return TYPECODE_UROWID;
        else 
            return 0;
    }

    //
    public static int to_typejbdc( String str )
    {
        if ( str.trim().equalsIgnoreCase( "ARRAY" ) )
            return TYPEJDBC_ARRAY;
        else if ( str.trim().equalsIgnoreCase( "BFILE" ) )
            return TYPEJDBC_BFILE;
        else if ( str.trim().equalsIgnoreCase( "BIGINT" ) )
            return TYPEJDBC_BIGINT;
        else if ( str.trim().equalsIgnoreCase( "BINARY" ) )
            return TYPEJDBC_BINARY;
        else if ( str.trim().equalsIgnoreCase( "BINARY_DOUBLE" ) )
            return TYPEJDBC_BINARY_DOUBLE;
        else if ( str.trim().equalsIgnoreCase( "BINARY_FLOAT" ) )
            return TYPEJDBC_BINARY_FLOAT;
        else if ( str.trim().equalsIgnoreCase( "BIT" ) )
            return TYPEJDBC_BIT;
        else if ( str.trim().equalsIgnoreCase( "BLOB" ) )
            return TYPEJDBC_BLOB;
        else if ( str.trim().equalsIgnoreCase( "BOOLEAN" ) )
            return TYPEJDBC_BOOLEAN;
        else if ( str.trim().equalsIgnoreCase( "CHAR" ) )
            return TYPEJDBC_CHAR;
        else if ( str.trim().equalsIgnoreCase( "CLOB" ) )
            return TYPEJDBC_CLOB;
        else if ( str.trim().equalsIgnoreCase( "CURSOR" ) )
            return TYPEJDBC_CURSOR;
        else if ( str.trim().equalsIgnoreCase( "DATALINK" ) )
            return TYPEJDBC_DATALINK;
        else if ( str.trim().equalsIgnoreCase( "DATE" ) )
            return TYPEJDBC_DATE;
        else if ( str.trim().equalsIgnoreCase( "DECIMAL" ) )
            return TYPEJDBC_DECIMAL;
        else if ( str.trim().equalsIgnoreCase( "DOUBLE" ) )
            return TYPEJDBC_DOUBLE;
        else if ( str.trim().equalsIgnoreCase( "FIXED_CHAR" ) )
            return TYPEJDBC_FIXED_CHAR;
        else if ( str.trim().equalsIgnoreCase( "FLOAT" ) )
            return TYPEJDBC_FLOAT;
        else if ( str.trim().equalsIgnoreCase( "INTEGER" ) )
            return TYPEJDBC_INTEGER;
        else if ( str.trim().equalsIgnoreCase( "INTERVAL_DS" ) )
            return TYPEJDBC_INTERVALDS;
        else if ( str.trim().equalsIgnoreCase( "INTERVAL_YM" ) )
            return TYPEJDBC_INTERVALYM;
        else if ( str.trim().equalsIgnoreCase( "JAVA_OBJECT" ) )
            return TYPEJDBC_JAVA_OBJECT;
        else if ( str.trim().equalsIgnoreCase( "JAVA_STRUCT" ) )
            return TYPEJDBC_JAVA_STRUCT;
        else if ( str.trim().equalsIgnoreCase( "LONGVARBINARY" ) )
            return TYPEJDBC_LONGVARBINARY;
        else if ( str.trim().equalsIgnoreCase( "LONGVARCHAR" ) )
            return TYPEJDBC_LONGVARCHAR;
        else if ( str.trim().equalsIgnoreCase( "NULL" ) )
            return TYPEJDBC_NULL;
        else if ( str.trim().equalsIgnoreCase( "NUMBER" ) )
            return TYPEJDBC_NUMBER;
        else if ( str.trim().equalsIgnoreCase( "NUMERIC" ) )
            return TYPEJDBC_NUMERIC;
        else if ( str.trim().equalsIgnoreCase( "OPAQUE" ) )
            return TYPEJDBC_OPAQUE;
        else if ( str.trim().equalsIgnoreCase( "OTHER" ) )
            return TYPEJDBC_OTHER;
        else if ( str.trim().equalsIgnoreCase( "PLSQL_INDEX_TABLE" ) )
            return TYPEJDBC_PLSQL_INDEX_TABLE;
        else if ( str.trim().equalsIgnoreCase( "RAW" ) )
            return TYPEJDBC_RAW;
        else if ( str.trim().equalsIgnoreCase( "REAL" ) )
            return TYPEJDBC_REAL;
        else if ( str.trim().equalsIgnoreCase( "REF" ) )
            return TYPEJDBC_REF;
        else if ( str.trim().equalsIgnoreCase( "ROWID" ) )
            return TYPEJDBC_ROWID;
        else if ( str.trim().equalsIgnoreCase( "SMALLINT" ) )
            return TYPEJDBC_SMALLINT;
        else if ( str.trim().equalsIgnoreCase( "STRUCT" ) )
            return TYPEJDBC_STRUCT;
        else if ( str.trim().equalsIgnoreCase( "TIME" ) )
            return TYPEJDBC_TIME;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMP" ) )
            return TYPEJDBC_TIMESTAMP;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMPLTZ" ) )
            return TYPEJDBC_TIMESTAMPLTZ;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMPNS" ) )
            return TYPEJDBC_TIMESTAMPNS;
        else if ( str.trim().equalsIgnoreCase( "TIMESTAMPTZ" ) )
            return TYPEJDBC_TIMESTAMPTZ;
        else if ( str.trim().equalsIgnoreCase( "TINYINT" ) )
            return TYPEJDBC_TINYINT;
        else if ( str.trim().equalsIgnoreCase( "VARBINARY" ) )
            return TYPEJDBC_VARBINARY;
        else if ( str.trim().equalsIgnoreCase( "VARCHAR" ) )
            return TYPEJDBC_VARCHAR;
        else 
            return 0;
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
            case TYPEJDBC_FIXED_CHAR:
            case TYPEJDBC_LONGVARBINARY:
            case TYPEJDBC_LONGVARCHAR:
            case TYPEJDBC_PLSQL_INDEX_TABLE:
            case TYPEJDBC_RAW:
            case TYPEJDBC_VARBINARY:
            case TYPEJDBC_VARCHAR:
                def = 1;
                break;

            case TYPEJDBC_BFILE:
            case TYPEJDBC_BLOB:
            case TYPEJDBC_CLOB:
            case TYPEJDBC_CURSOR:
            case TYPEJDBC_DATALINK:
            case TYPEJDBC_OPAQUE:
            case TYPEJDBC_OTHER:
            case TYPEJDBC_JAVA_OBJECT:
            case TYPEJDBC_JAVA_STRUCT:
            case TYPEJDBC_REF:
            case TYPEJDBC_STRUCT:
                def = -1;
                break;

            case TYPEJDBC_INTERVALDS:
            case TYPEJDBC_INTERVALYM:
                def = 2;
                break;

            case TYPEJDBC_BIGINT:
            case TYPEJDBC_BINARY_DOUBLE:
            case TYPEJDBC_BINARY_FLOAT:
            case TYPEJDBC_BIT:
            case TYPEJDBC_BOOLEAN:
            case TYPEJDBC_DATE:
            case TYPEJDBC_DECIMAL:
            case TYPEJDBC_DOUBLE:
            case TYPEJDBC_FLOAT:
            case TYPEJDBC_INTEGER:
            case TYPEJDBC_NULL:
            case TYPEJDBC_NUMBER:
            case TYPEJDBC_REAL:
            case TYPEJDBC_ROWID:
            case TYPEJDBC_SMALLINT:
            case TYPEJDBC_TIME:
            case TYPEJDBC_TIMESTAMP:
            case TYPEJDBC_TIMESTAMPLTZ:
            case TYPEJDBC_TIMESTAMPNS:
            case TYPEJDBC_TIMESTAMPTZ:
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
            case TYPEJDBC_BINARY_DOUBLE:
            case TYPEJDBC_BINARY_FLOAT:
            case TYPEJDBC_DECIMAL:
            case TYPEJDBC_DOUBLE:
            case TYPEJDBC_FLOAT:
            case TYPEJDBC_INTEGER:
            case TYPEJDBC_NUMBER:
            case TYPEJDBC_REAL:
            case TYPEJDBC_SMALLINT:
            case TYPEJDBC_TINYINT:
                def = -127;
                break;

            case TYPEJDBC_TIME:
            case TYPEJDBC_TIMESTAMP:
            case TYPEJDBC_TIMESTAMPLTZ:
            case TYPEJDBC_TIMESTAMPNS:
            case TYPEJDBC_TIMESTAMPTZ:
                def = 6;
                break;

            case TYPEJDBC_ARRAY:
            case TYPEJDBC_BFILE:
            case TYPEJDBC_BINARY:
            case TYPEJDBC_BIT:
            case TYPEJDBC_BLOB:
            case TYPEJDBC_BOOLEAN:
            case TYPEJDBC_CHAR:
            case TYPEJDBC_CLOB:
            case TYPEJDBC_CURSOR:
            case TYPEJDBC_DATALINK:
            case TYPEJDBC_DATE:
            case TYPEJDBC_FIXED_CHAR:
            case TYPEJDBC_INTERVALDS:
            case TYPEJDBC_INTERVALYM:
            case TYPEJDBC_JAVA_OBJECT:
            case TYPEJDBC_JAVA_STRUCT:
            case TYPEJDBC_LONGVARBINARY:
            case TYPEJDBC_LONGVARCHAR:
            case TYPEJDBC_NULL:
            case TYPEJDBC_OPAQUE:
            case TYPEJDBC_OTHER:
            case TYPEJDBC_PLSQL_INDEX_TABLE:
            case TYPEJDBC_REF:
            case TYPEJDBC_ROWID:
            case TYPEJDBC_STRUCT:
            case TYPEJDBC_VARBINARY:
            case TYPEJDBC_VARCHAR:
            default:
                def = 0;
                break;
        }

        return def;
    }
};
