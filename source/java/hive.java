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
import java.math.*;
import java.util.*;
import java.security.*;

import oracle.sql.*;
import oracle.jdbc.*;

//
@SuppressWarnings("deprecation")
public class hive implements SQLData
{
    //
    private static hive_manager manager_;

    //
    private static String sql_;        // needed for SQLData inheritence
    private static BigDecimal key_;    //

    //
    final static BigDecimal SUCCESS = new BigDecimal( 0 );
    final static BigDecimal FAILURE = new BigDecimal( 1 );

    // override (SQLData inheritence)
    public String getSQLTypeName()
        throws SQLException 
    {
        log.trace( "getSQLTypeName called" );
        return sql_;
    }

    // override (SQLData inheritence)
    public void readSQL( SQLInput stream, String type )
        throws SQLException 
    {
        log.trace( "readSQL called" );
        sql_ = type;
        key_ = stream.readBigDecimal();
    }

    // override (SQLData inheritence)
    public void writeSQL( SQLOutput stream )
        throws SQLException 
    {
        log.trace( "writeSQL called" );
        stream.writeBigDecimal( key_ );
    }

    //
    static public BigDecimal SqlDesc( oracle.sql.ARRAY[] attr,  // out
                                      String             stmt,  // on
                                      oracle.sql.ARRAY   bnds, 
                                      oracle.sql.STRUCT  conn )
        throws SQLException, hive_exception
    {
        log.trace( "SqlDesc called" );

        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = new hive_context( stmt, bnds, conn );

        if ( ctx == null )
            throw new hive_exception( "Context not created for SqlDesc" );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        ResultSetMetaData rmd = ctx.descSql();
        log.trace( "SqlDesc: columns: " + rmd.getColumnCount() );

        if ( rmd.getColumnCount() > 0 )
        {
            int cset = dbms_types.nls_charset_id();
            int cfrm = dbms_types.nls_charset_format();

            for ( int i = 1; i <= rmd.getColumnCount(); ++i ) 
            {
                hive_attribute atr = new hive_attribute();

                atr.name = rmd.getColumnName( i );
                atr.code = dbms_types.to_dbms_type( rmd.getColumnType( i ) );

                switch ( atr.code )
                {
                    case dbms_types.TYPECODE_VARCHAR2:
                        {
                            if ( rmd.getPrecision( i ) > 4000 )
                                atr.len = 4000;
                            else
                            {
                                if ( rmd.getPrecision( i ) > 0 )
                                    atr.len = rmd.getPrecision( i );
                                else
                                    atr.len = -1;
                            }
                        }
                        atr.csid = cset;
                        atr.csfrm = cfrm;
                        atr.prec = -1;
                        atr.scale = -1;
                        break;

                    case dbms_types.TYPECODE_NUMBER:
                        atr.prec = rmd.getPrecision( i );
                        atr.scale = rmd.getScale( i );

                        if ( ( atr.prec == 0 ) && ( atr.scale == 0 ) )
                            atr.scale = -127;

                        if ( atr.prec == -1 ) 
                        {
                            atr.prec = 0;
                            atr.scale = -127;
                        }

                        if ( atr.scale == -1 )
                            atr.scale = -127;

                        break;

                    case dbms_types.TYPECODE_CLOB:
                        atr.len = rmd.getPrecision( i );
                        atr.csid = cset;
                        atr.csfrm = cfrm;
                        break;

                    case dbms_types.TYPECODE_BLOB:
                        atr.len = rmd.getPrecision( i );
                        break;

                    case dbms_types.TYPECODE_DATE:
                        break;

                    case dbms_types.TYPECODE_TIMESTAMP:
                    case dbms_types.TYPECODE_TIMESTAMP_TZ:
                    case dbms_types.TYPECODE_TIMESTAMP_LTZ:
                        atr.prec = 0;
                        atr.scale = 6;
                        break;

                    case dbms_types.TYPECODE_OBJECT:
                    default:
                        break;
                }

                Object[] obj = new Object[] { new String( atr.name ),
                                              new Integer( atr.code ),
                                              new Integer( atr.prec ),
                                              new Integer( atr.scale ),
                                              new Integer( atr.len ),
                                              new Integer( atr.csid ),
                                              new Integer( atr.csfrm ) };

                StructDescriptor ids = StructDescriptor.createDescriptor( "ATTRIBUTE", con );
                STRUCT itm = new STRUCT( ids, con, obj );
                col.add( itm );
            }
        }

        ArrayDescriptor des = ArrayDescriptor.createDescriptor( "ATTRIBUTES", con );
        STRUCT[] dat = col.toArray( new STRUCT[ col.size() ] );

        attr[0] = new ARRAY( des, con, dat );

        return SUCCESS;
    }

    //
    static public BigDecimal SqlDesc( oracle.sql.ARRAY[] attr,  // out
                                      BigDecimal         key )  // in
        throws SQLException, hive_exception
    {
        log.trace( "SqlDesc called" );

        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = manager_.getContext( key );

        if ( ctx == null )
            throw new hive_exception( "Context not found for SqlDesc" );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        ResultSetMetaData rmd = ctx.descSql();
        log.trace( "SqlDesc: columns: " + rmd.getColumnCount() );

        if ( rmd.getColumnCount() > 0 )
        {
            for ( int i = 1; i <= rmd.getColumnCount(); ++i ) 
            {
                hive_attribute atr = new hive_attribute();

                atr.name = rmd.getColumnName( i );
                atr.code = dbms_types.to_dbms_type( rmd.getColumnType( i ) );

                switch ( atr.code )
                {
                    case dbms_types.TYPECODE_VARCHAR2:
                        {
                            if ( rmd.getPrecision( i ) > 4000 )
                                atr.len = 4000;
                            else
                            {
                                if ( rmd.getPrecision( i ) > 0 )
                                    atr.len = rmd.getPrecision( i );
                                else
                                    atr.len = -1;
                            }
                        }
                        break;

                    case dbms_types.TYPECODE_NUMBER:
                        atr.prec = rmd.getPrecision( i );
                        atr.scale = rmd.getScale( i );
                        break;

                    case dbms_types.TYPECODE_CLOB:
                        atr.len = rmd.getPrecision( i );
                        break;

                    case dbms_types.TYPECODE_BLOB:
                        atr.len = rmd.getPrecision( i );
                        break;

                    case dbms_types.TYPECODE_DATE:
                        break;

                    case dbms_types.TYPECODE_OBJECT:
                    default:
                        break;
                }

                Object[] obj = new Object[] { new String( atr.name ),
                                              new Integer( atr.code ),
                                              new Integer( atr.prec ),
                                              new Integer( atr.scale ),
                                              new Integer( atr.len ),
                                              new Integer( atr.csid ),
                                              new Integer( atr.csfrm ) };

                StructDescriptor ids = StructDescriptor.createDescriptor( "ATTRIBUTE", con );
                STRUCT itm = new STRUCT( ids, con, obj );
                col.add( itm );
            }
        }

        ArrayDescriptor des = ArrayDescriptor.createDescriptor( "ATTRIBUTES", con );
        STRUCT[] dat = col.toArray( new STRUCT[ col.size() ] );

        attr[0] = new ARRAY( des, con, dat );

        return SUCCESS;
    }

    //
    static public BigDecimal SqlOpen( String            stmt,
                                      oracle.sql.ARRAY  bnds,
                                      oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        log.trace( "SqlOpen called [String]: " + stmt );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            log.trace( "SqlOpen created hive_manager" );
        }

        hive_context ctx = new hive_context( stmt, bnds, conn );
        key_ = manager_.createContext( ctx );

        log.trace( "SqlOpen returning key: " + key_ );

        return key_;
    }

    //
    static public BigDecimal SqlOpen( STRUCT[]          sctx,
                                      String            stmt,
                                      oracle.sql.ARRAY  bnds,
                                      oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        log.trace( "SqlOpen called [STRUCT]: " + sctx );
        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        //
        key_ = SqlOpen( stmt, bnds, conn );

        if ( key_.intValue() == 0 )
            return FAILURE;

        //
        Object[] imp = new Object[ 1 ];
        imp[ 0 ] = key_;

        StructDescriptor dsc = new StructDescriptor( "HIVE_T", con );
        sctx[ 0 ] = new STRUCT( dsc, con, imp );

        return SUCCESS;
    }

    //
    static public BigDecimal SqlOpen( BigDecimal[]      key,
                                      String            stmt,
                                      oracle.sql.ARRAY  bnds,
                                      oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        log.trace( "SqlOpen called" );

        key_ = SqlOpen( stmt, bnds, conn );

        if ( key_.intValue() == 0 )
            return FAILURE;

        key[ 0 ] = key_;

        return SUCCESS;
    }

    static public BigDecimal SqlFetch( ARRAY[]    out,
                                       BigDecimal key, 
                                       BigDecimal num )
        throws SQLException, InvalidKeyException, hive_exception
    {
        log.trace( "SqlFetch called: key_ = " + key );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        if ( manager_ == null )
            manager_ = new hive_manager();

        hive_context ctx = manager_.getContext( key );

        if ( ctx == null )
            throw new hive_exception( "Context not found for SqlFetch" );

        //
        if ( ! ctx.ready() )
        {
            if ( ! ctx.execute() )
                return FAILURE;
        }

        //
        StructDescriptor dsc = new StructDescriptor( "DATA", con );

        if ( ctx.next() )
        {
            int cnt = ctx.columnCount();
            Object[] cols = new Object[ cnt ];

            for ( int c = 1; c <= cnt; ++c )
            {
                Object col = ctx.getObject( c );
                int typ = dbms_types.to_dbms_type( ctx.columnType( c ) );

                Object[] atr =
                {
                    new BigDecimal( typ ),                                  // type code
                    ( typ == dbms_types.TYPECODE_VARCHAR2 )  ? col : null,  // val_varchar2
                    ( typ == dbms_types.TYPECODE_NUMBER )    ? col : null,  // val_number
                    ( typ == dbms_types.TYPECODE_DATE )      ? col : null,  // val_date
                    ( typ == dbms_types.TYPECODE_TIMESTAMP ) ? col : null,  // val_timestamp
                    ( typ == dbms_types.TYPECODE_CLOB )      ? col : null,  // val_clob
                    ( typ == dbms_types.TYPECODE_BLOB )      ? col : null   // val_blob
                };

                //
                cols[ c - 1 ] = new STRUCT( dsc, con, atr );
            }

            //
            ArrayDescriptor ary = ArrayDescriptor.createDescriptor( "RECORDS", con );

            ARRAY arr = new ARRAY( ary, con, cols );
            out[ 0 ] = arr;
        }
        else
            out[ 0 ] = null;

        return SUCCESS;
    }

    //
    static public BigDecimal SqlClose( BigDecimal key )
        throws SQLException, InvalidKeyException
    {
        log.trace( "SqlClose called" );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            log.trace( "SqlClose created hive_manager" );
        }

        hive_context ctx = manager_.removeContext( key );

        if ( ctx != null )
            ctx.clear();

        return SUCCESS;
    }

    //
    static public void SqlDml( String stmt, oracle.sql.ARRAY bnds, oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        hive_context ctx = new hive_context( stmt, bnds, conn );

        if ( ctx == null )
            throw new hive_exception( "Context not created for SqlDml" );

        ctx.executeDML();
    }

    //
    static public void SqlDdl( String stmt, oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        hive_context ctx = new hive_context( stmt, null, conn );

        if ( ctx == null )
            throw new hive_exception( "Context not created for SqlDml" );

        ctx.executeDDL();
    }
};

