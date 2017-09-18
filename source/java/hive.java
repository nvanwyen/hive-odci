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
    {
        return sql_;
    }

    // override (SQLData inheritence)
    public void readSQL( SQLInput stream, String type )
        throws SQLException 
    {
        try
        {
            sql_ = type;
            key_ = stream.readBigDecimal();
        }
        catch ( SQLException ex )
        {
            log.error( "hive::readSQL SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::readSQL Exception: " + log.stack( ex ) );
            throw ex;
        }
    }

    // override (SQLData inheritence)
    public void writeSQL( SQLOutput stream )
        throws SQLException 
    {
        try
        {
            stream.writeBigDecimal( key_ );
        }
        catch ( SQLException ex )
        {
            log.error( "hive::writeSQL SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::writeSQL Exception: " + log.stack( ex ) );
            throw ex;
        }
    }

    //
    static public BigDecimal SqlDesc( oracle.sql.ARRAY[] attr,  // out
                                      String             stmt,  // in
                                      oracle.sql.ARRAY   bnds, 
                                      oracle.sql.STRUCT  conn )
        throws SQLException, hive_exception
    {
        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = new hive_context( stmt, bnds, conn );

        if ( ctx == null )
            throw new hive_exception( "Context not created for SqlDesc( attr, stmt, bnds, conn )" );
        else
            log.trace( "hive::SqlDesc hive_context [created]: " + ctx.toString() );

        try
        {
            Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

            ResultSetMetaData rmd = ctx.descSql();

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
        }
        catch ( SQLException ex )
        {
            log.error( "hive::SqlDesc SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::SqlDesc Exception: " + log.stack( ex ) );
            throw ex;
        }

        return SUCCESS;
    }

    //
    static public BigDecimal SqlDesc( oracle.sql.ARRAY[] attr,  // out
                                      BigDecimal         key )  // in
        throws SQLException, hive_exception
    {
        ArrayList<STRUCT> col = new ArrayList<STRUCT>();
        hive_context ctx = manager_.getContext( key );

        if ( ctx == null )
            throw new hive_exception( "Context not found for SqlDesc( attr, key )" );
        else
            log.trace( "hive::SqlDesc hive_context [managed]: " + ctx.toString() );

        try
        {
            Connection con = DriverManager.getConnection( "jdbc:default:connection:" );

            ResultSetMetaData rmd = ctx.descSql();

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
        }
        catch ( SQLException ex )
        {
            log.error( "hive::SqlDesc SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::SqlDesc Exception: " + log.stack( ex ) );
            throw ex;
        }

        return SUCCESS;
    }

    //
    static public BigDecimal SqlOpen( String            stmt,
                                      oracle.sql.ARRAY  bnds,
                                      oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        try
        {
            if ( manager_ == null )
                manager_ = new hive_manager();

            hive_context ctx = new hive_context( stmt, bnds, conn );
            key_ = manager_.createContext( ctx );

            if ( ctx != null )
                log.trace( "hive::SqlOpen hive_context [created]: " + ctx.toString() );

            return key_;
        }
        catch ( SQLException ex )
        {
            log.error( "hive::SqlOpen SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::SqlOpen Exception: " + log.stack( ex ) );
            throw ex;
        }
    }

    //
    static public BigDecimal SqlOpen( STRUCT[]          sctx,
                                      String            stmt,
                                      oracle.sql.ARRAY  bnds,
                                      oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        try
        {
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
        catch ( SQLException ex )
        {
            log.error( "hive::SqlOpen SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::SqlOpen Exception: " + log.stack( ex ) );
            throw ex;
        }
    }

    //
    static public BigDecimal SqlOpen( BigDecimal[]      key,
                                      String            stmt,
                                      oracle.sql.ARRAY  bnds,
                                      oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        try
        {
            key_ = SqlOpen( stmt, bnds, conn );

            if ( key_.intValue() == 0 )
                return FAILURE;

            key[ 0 ] = key_;

            return SUCCESS;
        }
        catch ( SQLException ex )
        {
            log.error( "hive::SqlOpen SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::SqlOpen Exception: " + log.stack( ex ) );
            throw ex;
        }
    }

    static public BigDecimal SqlFetch( ARRAY[]    out,
                                       BigDecimal key, 
                                       BigDecimal num )
        throws SQLException, InvalidKeyException, hive_exception
    {
        try
        {
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
                Object col = null;
                Object[] cols = new Object[ cnt ];

                for ( int c = 1; c <= cnt; ++c )
                {
                    int typ = 0;

                    if ( ctx.colRuleMapped( c ) )
                        typ = ctx.columnType( c );
                    else
                        typ = hive_types.to_dbms_type( ctx.columnType( c ) );

                    try
                    {
                        //
                        col = ctx.getObject( c );

                        switch ( typ )
                        {
                            // 
                            case hive_types.TYPECODE_CLOB:
                                col = (Object)hive_types.to_clob( (String)col );
                                break;

                            //
                            case hive_types.TYPECODE_BLOB:
                                col = (Object)hive_types.to_blob( col );
                                break;

                            // other
                            default:
                                break;
                        }
                    }
                    catch ( SQLException ex )
                    {
                        log.warn( "hive::SqlFetch SQLException building [" + hive_types.to_typecode( typ ) + "]: "
                                                                           + log.stack( ex ) + log.code( ex ) );
                    }
                    catch ( Exception ex )
                    {
                        log.warn( "hive::SqlFetch Exception building [" + hive_types.to_typecode( typ ) + "]: "
                                                                        + log.stack( ex ) );
                    }

                    //
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
        catch ( SQLException ex )
        {
            log.error( "hive::SqlFetch SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::SqlFetch Exception: " + log.stack( ex ) );
            throw ex;
        }
    }

    //
    static public BigDecimal SqlClose( BigDecimal key )
        throws InvalidKeyException, hive_exception
    {
        try
        {
            if ( manager_ == null )
            {
                manager_ = new hive_manager();
            }

            hive_context ctx = manager_.getContext( key );

            if ( ctx != null )
            {
                log.trace( "hive::SqlClose hive_context [managed]: " + ctx.toString() );
                ctx.clear();
            }

            return SUCCESS;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::SqlClose Exception: " + log.stack( ex ) );
            throw ex;
        }
    }

    //
    static public void SqlDml( String stmt, oracle.sql.ARRAY bnds, oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        try
        {
            hive_context ctx = new hive_context( stmt, bnds, conn );

            if ( ctx == null )
                throw new hive_exception( "Context not created for SqlDml( stmt, bnds, conn )" );
            else
                log.trace( "hive::SqlDml hive_context [created]: " + ctx.toString() );

            ctx.executeDML();
        }
        catch ( SQLException ex )
        {
            log.error( "hive::SqlDml SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::SqlDml Exception: " + log.stack( ex ) );
            throw ex;
        }
    }

    //
    static public void SqlDdl( String stmt, oracle.sql.STRUCT conn )
        throws SQLException, hive_exception
    {
        try
        {
            hive_context ctx = new hive_context( stmt, null, conn );

            if ( ctx == null )
                throw new hive_exception( "Context not created for SqlDml( stmt, conn ) " );
            else
                log.trace( "hive::SqlDdl hive_context [created]: " + ctx.toString() );

            ctx.executeDDL();
        }
        catch ( SQLException ex )
        {
            log.error( "hive::SqlDdl SQLException: " + log.stack( ex ) + log.code( ex ) );
            throw ex;
        }
        catch ( Exception ex )
        {
            //
            log.error( "hive::SqlDdl Exception: " + log.stack( ex ) );
            throw ex;
        }
    }
};

