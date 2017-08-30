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
        //log.trace( "hive::getSQLTypeName called" );
        return sql_;
    }

    // override (SQLData inheritence)
    public void readSQL( SQLInput stream, String type )
        throws SQLException 
    {
        //log.trace( "hive::readSQL called" );
        sql_ = type;
        key_ = stream.readBigDecimal();
    }

    // override (SQLData inheritence)
    public void writeSQL( SQLOutput stream )
        throws SQLException 
    {
        //log.trace( "hive::writeSQL called" );
        stream.writeBigDecimal( key_ );
    }

    //
    static public BigDecimal SqlDesc( oracle.sql.ARRAY[] attr,  // out
                                      String             stmt,  // in
                                      oracle.sql.ARRAY   bnds, 
                                      oracle.sql.STRUCT  conn )
        throws SQLException, hive_exception
    {
        //log.trace( "hive::SqlDesc( attr, stmt, bnds, conn ) called" );

        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = new hive_context( stmt, bnds, conn );
        //log.trace( "hive::SqlDesc( attr, stmt, bnds, conn ) - created: " + ctx.toString() );

        if ( ctx == null )
            throw new hive_exception( "Context not created for SqlDesc( attr, stmt, bnds, conn )" );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        ResultSetMetaData rmd = ctx.descSql();
        //log.trace( "hive::SqlDesc( attr, stmt, bnds, conn ): columns: " + rmd.getColumnCount() );

        if ( rmd.getColumnCount() > 0 )
        {
            int cset = hive_types.nls_charset_id();
            int cfrm = hive_types.nls_charset_format();

            for ( int i = 1; i <= rmd.getColumnCount(); ++i ) 
            {
                hive_attribute atr = new hive_attribute();

                atr.name = rmd.getColumnName( i );

                // if the column is "mapped", then get the type, precision, scale, etc...
                // from the context, and not the resulting metadata
                //
                if ( ctx.colRuleMapped( atr.name ) )
                {
                    //
                    //log.trace( "hive::SqlDesc processing mapped column: " + atr.name );

                    // get mapping data ...
                    atr.code  = ctx.colRuleTypeCode( atr.name );
                    atr.len   = ctx.colRuleLength( atr.name );
                    atr.prec  = ctx.colRulePrecision( atr.name );
                    atr.scale = ctx.colRuleScale( atr.name );

                    atr.csid = cset;
                    atr.csfrm = cfrm;

                    // translate values based on code
                    atr = ctx.colRuleAttr( atr );
                }
                else
                {
                    atr.code = hive_types.to_dbms_type( rmd.getColumnType( i ) );

                    switch ( atr.code )
                    {
                        case hive_types.TYPECODE_VARCHAR2:
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

                        case hive_types.TYPECODE_NUMBER:
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

                        case hive_types.TYPECODE_CLOB:
                            atr.len = rmd.getPrecision( i );
                            atr.csid = cset;
                            atr.csfrm = cfrm;
                            break;

                        case hive_types.TYPECODE_BLOB:
                            atr.len = rmd.getPrecision( i );
                            break;

                        case hive_types.TYPECODE_DATE:
                            break;

                        case hive_types.TYPECODE_TIMESTAMP:
                        case hive_types.TYPECODE_TIMESTAMP_TZ:
                        case hive_types.TYPECODE_TIMESTAMP_LTZ:
                            atr.prec = 0;
                            atr.scale = 6;
                            break;

                        case hive_types.TYPECODE_OBJECT:
                        default:
                            break;
                    }
                }

                //
                //log.trace( "hive::SqlDesc Stm ATTRIBUTE name:  " + atr.name   + "\n" +
                //           "+                           code:  " + atr.code   + " [" + hive_types.to_typecode( atr.code ) + "]\n" +
                //           "+                           prec:  " + atr.prec   + "\n" +
                //           "+                           scale: " + atr.scale  + "\n" +
                //           "+                           len:   " + atr.len    + "\n" +
                //           "+                           csid:  " + atr.csid   + "\n" +
                //           "+                           csfrm: " + atr.csfrm );

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
        //log.trace( "hive::SqlDesc( attr, key ) called key: " + key.toString() );

        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = manager_.getContext( key );
        //log.trace( "hive::SqlDesc( attr, key ) - retrieved: " + ctx.toString() );

        if ( ctx == null )
            throw new hive_exception( "Context not found for SqlDesc( attr, key )" );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        ResultSetMetaData rmd = ctx.descSql();
        //log.trace( "hive::SqlDesc( attr, key ): columns: " + rmd.getColumnCount() );

        if ( rmd.getColumnCount() > 0 )
        {
            int cset = hive_types.nls_charset_id();
            int cfrm = hive_types.nls_charset_format();

            for ( int i = 1; i <= rmd.getColumnCount(); ++i ) 
            {
                hive_attribute atr = new hive_attribute();

                atr.name = rmd.getColumnName( i );

                // if the column is "mapped", then get the type, precision, scale, etc...
                // from the context, and not the resulting metadata
                //
                if ( ctx.colRuleMapped( atr.name ) )
                {
                    //
                    //log.trace( "hive::SqlDesc processing mapped column: " + atr.name );

                    // get mapping data ...
                    atr.code  = ctx.colRuleTypeCode( atr.name );
                    atr.len   = ctx.colRuleLength( atr.name );
                    atr.prec  = ctx.colRulePrecision( atr.name );
                    atr.scale = ctx.colRuleScale( atr.name );

                    atr.csid = cset;
                    atr.csfrm = cfrm;

                    // translate values based on code
                    atr = ctx.colRuleAttr( atr );
                }
                else
                {
                    atr.code = hive_types.to_dbms_type( rmd.getColumnType( i ) );

                    switch ( atr.code )
                    {
                        case hive_types.TYPECODE_VARCHAR2:
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

                        case hive_types.TYPECODE_NUMBER:
                            atr.prec = rmd.getPrecision( i );
                            atr.scale = rmd.getScale( i );
                            break;

                        case hive_types.TYPECODE_CLOB:
                            atr.len = rmd.getPrecision( i );
                            break;

                        case hive_types.TYPECODE_BLOB:
                            atr.len = rmd.getPrecision( i );
                            break;

                        case hive_types.TYPECODE_DATE:
                            break;

                        case hive_types.TYPECODE_OBJECT:
                        default:
                            break;
                    }
                }

                //
                //log.trace( "hive::SqlDesc Key ATTRIBUTE name:  " + atr.name   + "\n" +
                //           "+                           code:  " + atr.code   + " [" + hive_types.to_typecode( atr.code ) + "]\n" +
                //           "+                           prec:  " + atr.prec   + "\n" +
                //           "+                           scale: " + atr.scale  + "\n" +
                //           "+                           len:   " + atr.len    + "\n" +
                //           "+                           csid:  " + atr.csid   + "\n" +
                //           "+                           csfrm: " + atr.csfrm );

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
        //log.trace( "hive::SqlOpen( stmt, bnds, conn ) called [String]: " + stmt );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            //log.trace( "hive::SqlOpen( stmt, bnds, conn ) created hive_manager" );
        }

        hive_context ctx = new hive_context( stmt, bnds, conn );
        //log.trace( "hive::SqlOpen( stmt, bnds, conn ) - created: " + ctx.toString() );

        key_ = manager_.createContext( ctx );

        //log.trace( "hive::SqlOpen( stmt, bnds, conn ) returning key: " + key_ );
        return key_;
    }

    //
    static public BigDecimal SqlOpen( STRUCT[]          sctx,
                                      String            stmt,
                                      oracle.sql.ARRAY  bnds,
                                      oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        //log.trace( "hive::SqlOpen( sctx, stmt, bnds, conn ) called [STRUCT]: " + sctx );
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
        //log.trace( "hive::SqlOpen( key, stmt, bnds, conn ) called" );

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
        //log.trace( "hive::SqlFetch called: key_ = " + key );

        Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

        if ( manager_ == null )
            manager_ = new hive_manager();

        hive_context ctx = manager_.getContext( key );
        //log.trace( "hive::SqlFetch - retrieved: " + ctx.toString() );

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
                Object col = null;
                int typ = 0;


                if ( ctx.colRuleMapped( c ) )
                    typ = ctx.columnType( c );
                else
                    typ = hive_types.to_dbms_type( ctx.columnType( c ) );

                try
                {
                    switch ( typ )
                    {
                        case hive_types.TYPECODE_CLOB:
                            col = (Object)hive_types.to_clob( (String)ctx.getObject( c ) );
                            break;

                        case hive_types.TYPECODE_BLOB:
                            col = (Object)hive_types.to_blob( ctx.getObject( c ) );
                            break;

                        // others?
                        default:
                            col = ctx.getObject( c );
                    }
                }
                catch ( SQLException ex )
                {
                    log.warn( "hive::SqlFetch SQLException building [" + hive_types.to_typecode( typ ) + "]: " + ex.getMessage() );
                    col = ctx.getObject( c );
                }
                catch ( Exception ex )
                {
                    log.warn( "hive::SqlFetch Exception building [" + hive_types.to_typecode( typ ) + "]: " + ex.getMessage() );
                    col = ctx.getObject( c );
                }

                Object[] atr =
                {
                    new BigDecimal( typ ),                                  // type code
                    ( typ == hive_types.TYPECODE_VARCHAR2 )  ? col : null,  // val_varchar2
                    ( typ == hive_types.TYPECODE_NUMBER )    ? col : null,  // val_number
                    ( typ == hive_types.TYPECODE_DATE )      ? col : null,  // val_date
                    ( typ == hive_types.TYPECODE_TIMESTAMP ) ? col : null,  // val_timestamp
                    ( typ == hive_types.TYPECODE_CLOB )      ? col : null,  // val_clob
                    ( typ == hive_types.TYPECODE_BLOB )      ? col : null   // val_blob
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
        throws SQLException, InvalidKeyException, hive_exception
    {
        //log.trace( "hive::SqlClose( key ) called" );

        if ( manager_ == null )
        {
            manager_ = new hive_manager();
            //log.trace( "hive::SqlClose created hive_manager" );
        }

        hive_context ctx = manager_.getContext( key );
        //log.trace( "hive::SqlClose - retrieved: " + ctx.toString() );

        if ( ctx != null )
            ctx.clear();

        return SUCCESS;
    }

    //
    static public void SqlDml( String stmt, oracle.sql.ARRAY bnds, oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        //log.trace( "hive::SqlDml( stmt, bnds, conn ) called" );

        hive_context ctx = new hive_context( stmt, bnds, conn );
        //log.trace( "hive::SqlDml( stmt, bnds, conn ) - created: " + ctx.toString() );

        if ( ctx == null )
            throw new hive_exception( "Context not created for SqlDml( stmt, bnds, conn )" );

        ctx.executeDML();
    }

    //
    static public void SqlDdl( String stmt, oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        //log.trace( "hive::SqlDdl( stmt, conn ) called" );

        hive_context ctx = new hive_context( stmt, null, conn );
        //log.trace( "hive::SqlDdl( stmt, conn ) - created: " + ctx.toString() );

        if ( ctx == null )
            throw new hive_exception( "Context not created for SqlDml( stmt, conn ) " );

        ctx.executeDDL();
    }
};

