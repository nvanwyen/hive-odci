/*
 Copyright (c) 2016, Metasystems Technologies Inc (MTI), Nicholas Van Wyen
 All rights reserved.
 
 Redistribution and use in source and binary forms, with or without 
 modification, are permitted provided that the following conditions are met:
 
 1. Redistributions of source code must retain the above copyright notice, this
    list of conditions and the following disclaimer.
 
 2. Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.
 
 3. Neither the name of the copyright holder nor the names of its contributors
    may be used to endorse or promote products derived from this software
    without specific prior written permission.
 
 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/


package oracle.mti.odci;

//
import java.io.*;
import java.net.*;
import java.sql.*;
import java.lang.*;

import oracle.sql.*;
import oracle.jdbc.*;

//
@SuppressWarnings("deprecation")
public class dbms_types
{
    /*
        This java class duplicates the PL/SQL SYS.dbms_types
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
            log.error( "dbms_types::nls_charset_id SQLException: " + ex.getMessage() );
        }
        catch ( Exception ex )
        {
            //
            log.error( "dbms_types::nls_charset_id Exception: " + ex.getMessage() );
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
                log.error( "dbms_types::nls_charset_id (finally_block) SQLException: " + ex.getMessage() );
            }
            catch ( Exception ex )
            {
                log.error( "dbms_types::nls_charset_id (finally_block) Exception: " + ex.getMessage() );
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
};

