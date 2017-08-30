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
import java.util.regex.*;

import oracle.sql.*;
import oracle.jdbc.*;

//
@SuppressWarnings("deprecation")
public class hive_context
{
    //
    private static final String HINT_USE_MAP = hive_rule.SUPPORTED[ 0 ];

    //
    private hive_connection   con_;
    private hive_bindings     bnd_;

    //
    private String            sql_;
    private hive_rule         rul_;
    private PreparedStatement stm_;
    private ResultSet         rst_;
    private ResultSetMetaData rmd_;

    //
    private long rec_;

    // ctor
    //
    public hive_context( String sql, oracle.sql.ARRAY bnd, oracle.sql.STRUCT con ) throws SQLException, hive_exception
    {
        //log.trace( "hive_context::ctor: " + sql );

        rul_ = new hive_rule();
        sql_ = rul_.add( sql );

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( bnd != null )
            bnd_ = new hive_bindings( bnd );
        else
            bnd_ = new hive_bindings();

        if ( con != null )
            con_ = new hive_connection( con );
        else
            con_ = new hive_connection();

        con_.loadDriver();
        con_.createConnection();

        rec_ = 0;
    }

    public String toString()
    {
        String str = "";

        str +=                                 "\n";
        str += "... con: " + con_.toString() + "\n";
        str += "... bnd: " + bnd_.toString() + "\n";
        str += "... sql: " + sql_.toString() + "\n";
        str += "... rul: " + rul_.toString() + "\n";
        str += "... rec: " + String.format( "%d", rec_ ) + "\n";

        return str;
    }

    //
    public boolean ready()
    {
        //log.trace( "hive_context::ready: " + ( ! ( rst_ == null ) ) );
        return ( ! ( rst_ == null ) );
    }

    //
    public void clear()
    {
        //log.trace( "hive_context::clear" );

        // don't clear the connection, bindings, rules or sql ... only JDBC classes
        stm_ = null;
        rst_ = null;
        rmd_ = null;
    }

    // members
    //
    public String getSql()
    {
        return sql_;
    }

    //
    public PreparedStatement applyBindings( PreparedStatement stmt )
        throws SQLException, hive_exception
    {
        if ( bnd_ != null )
        {
            if ( bnd_.size() > 0 )
            {
                int idx = 0;

                //log.trace( "hive_context - Context binding data: " + bnd_.toString() );

                for ( hive_bind bnd : bnd_.binds )
                {
                    idx += 1;

                    switch ( bnd.type )
                    {
                        //
                        case hive_bind.TYPE_BOOL:

                            //log.trace( "hive_context::applyBindings: TYPE_BOOL" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setBoolean( idx, bnd.toBool() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        case hive_bind.TYPE_DATE:

                            //log.trace( "hive_context::applyBindings: TYPE_DATE" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setDate( idx, bnd.toDate() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        case hive_bind.TYPE_FLOAT:

                            //log.trace( "hive_context::applyBindings: TYPE_FLOAT" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setFloat( idx, bnd.toFloat() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        case hive_bind.TYPE_INT:

                            //log.trace( "hive_context::applyBindings: TYPE_INT" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setInt( idx, bnd.toInt() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        case hive_bind.TYPE_LONG:

                            //log.trace( "hive_context::applyBindings: TYPE_LONG" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setLong( idx, bnd.toLong() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        case hive_bind.TYPE_NULL:

                            //log.trace( "hive_context::applyBindings: TYPE_NULL" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setNull( idx, Types.VARCHAR );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Encountered OUT or INOUT scope for TYPE_NULL" );

                            break;

                        //
                        case hive_bind.TYPE_ROWID:

                            //log.trace( "hive_context::applyBindings: TYPE_ROWID" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setString( idx, bnd.toRowid() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        case hive_bind.TYPE_SHORT:

                            //log.trace( "hive_context::applyBindings: TYPE_SHORT" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setShort( idx, bnd.toShort() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        case hive_bind.TYPE_STRING:

                            //log.trace( "hive_context::applyBindings: TYPE_STRING" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setString( idx, bnd.toVarchar() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        case hive_bind.TYPE_TIME:

                            //log.trace( "hive_context::applyBindings: TYPE_TIME" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setTime( idx, bnd.toTime() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        case hive_bind.TYPE_TIMESTAMP:

                            //log.trace( "hive_context::applyBindings: TYPE_TIMESTAMP" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setTimestamp( idx, bnd.toTimestamp() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        case hive_bind.TYPE_URL:

                            //log.trace( "hive_context::applyBindings: TYPE_URL" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setURL( idx, bnd.toUrl() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );

                            break;

                        //
                        default:
                            throw new hive_exception( "Unknown binding type [" + bnd.type + "] encountered" );
                    }
                }
            }
            else
                log.info( "hive_context - Context binding size zero (0), nothing to apply" );
        }
        else
            log.info( "hive_context - Context binding data is NULL, nothing can be applied" );

        return stmt;
    }

    //
    public PreparedStatement getPreparedStatement() throws SQLException, hive_exception
    {
        setPreparedStatement(); return stm_;
    }

    //
    public ResultSet getResultSet() throws SQLException, hive_exception
    {
        setResultSet(); return rst_;
    }

    //
    public ResultSetMetaData getResultSetMetaData() throws SQLException, hive_exception
    {
        setResultSetMetaData(); return rmd_;
    }

    // metadata
    //
    public int columnCount() throws SQLException, hive_exception
    {
        if ( rmd_ == null )
        {
            if ( rst_ == null )
                setResultSet();

            rmd_ = rst_.getMetaData();
        }

        //log.trace( "hive_context::columnCount rmd_: " + rmd_ );
        return rmd_.getColumnCount();
    }

    //
    public int columnType( int i ) throws SQLException, hive_exception
    {
        int typ = 0;

        //log.trace( "hive_context::columnType retrieving index: " + i );

        if ( rmd_ == null )
        {
            if ( rst_ == null )
                setResultSet();

            rmd_ = rst_.getMetaData();
        }

        if ( colRuleMapped( rmd_.getColumnName( i ) ) )
        {
            //log.trace( "hive_context::columnType getting mapped type from rul_: " + rul_ );
            typ = colRuleTypeCode( rmd_.getColumnName( i ) );
        }
        else
        {
            //log.trace( "hive_context::columnType getting native type from rmd_: " + rmd_ );
            typ = rmd_.getColumnType( i );
        }

        //log.trace( "hive_context::columnType returning: " + typ );
        return typ;
    }

    //
    public int rawType( int i ) throws SQLException, hive_exception
    {
        int typ = 0;

        if ( rmd_ == null )
        {
            if ( rst_ == null )
                setResultSet();

            rmd_ = rst_.getMetaData();
        }

        typ = rmd_.getColumnType( i );

        //log.trace( "hive_context::rawType returning: " + typ );
        return typ;
    }

    // recordset
    //
    public boolean next() throws SQLException
    {
        ++rec_;

        //log.trace( "hive_context::next rst_: " + rst_ );
        return rst_.next();
    }

    public long fetched()
    {
        return rec_;
    }

    //
    public int rowNumber() throws SQLException
    {
        return ( ready() ) ? rst_.getRow() : -1;
    }

    // data
    //
    public Object getObject( int i ) throws SQLException
    {
        return rst_.getObject( i );
    }

    //
    public Object getObject( String c ) throws SQLException
    {
        return rst_.getObject( c );
    }

    //
    public BigDecimal getBigDecimal( int i ) throws SQLException
    {
        return rst_.getBigDecimal( i );
    }

    //
    public BigDecimal getBigDecimal( String c ) throws SQLException
    {
        return rst_.getBigDecimal( c );
    }

    //
    public boolean getBoolean( int i ) throws SQLException
    {
        return rst_.getBoolean( i );
    }

    //
    public boolean getBoolean( String c ) throws SQLException
    {
        return rst_.getBoolean( c );
    }

    //
    public int getInt( int i ) throws SQLException
    {
        return rst_.getInt( i );
    }

    //
    public int getInt( String c ) throws SQLException
    {
        return rst_.getInt( c );
    }

    //
    public long getLong( int i ) throws SQLException
    {
        return rst_.getLong( i );
    }

    //
    public long getLong( String c ) throws SQLException
    {
        return rst_.getLong( c );
    }

    //
    public float getFloat( int i ) throws SQLException
    {
        return rst_.getFloat( i );
    }

    //
    public float getFloat( String c ) throws SQLException
    {
        return rst_.getFloat( c );
    }

    //
    public java.sql.Date getDate( int i ) throws SQLException
    {
        return rst_.getDate( i );
    }

    //
    public java.sql.Date getDate( String c ) throws SQLException
    {
        return rst_.getDate( c );
    }

    //
    public String getString( int i ) throws SQLException
    {
        return rst_.getString( i );
    }

    //
    public String getString( String c ) throws SQLException
    {
        return rst_.getString( c );
    }

    //
    public java.sql.Timestamp getTimestamp( int i ) throws SQLException
    {
        return rst_.getTimestamp( i );
    }

    //
    public java.sql.Timestamp getTimestamp( String c ) throws SQLException
    {
        return rst_.getTimestamp( c );
    }

    //
    public boolean execute() throws SQLException, hive_exception
    {
        String limit = hive_parameter.value( "query_limit" );

        //log.trace( "hive_context::execute" );

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( limit != null )
        {
            //log.trace( "hive_context::execute query_limit: " + limit );

            try
            {
                sql_ = limitSql( sql_, Integer.parseInt( limit.trim() ) );
            }
            catch ( NumberFormatException ex )
            {
                log.error( "hive_context::execute NumberFormatException: " + ex.getMessage() );
                // ... do nothing
            }
        }

        return setResultSet();
    }

    //
    public boolean executeDML() throws SQLException, hive_exception
    {
        boolean ok = false;

        //log.trace( "hive_context::executeDML" );

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No DML defined for hive context" );

        try
        {
            PreparedStatement stm = applyBindings( con_.getConnection().prepareStatement( sql_ ) );
            stm.executeUpdate();
            con_.getConnection().commit();
            ok = true;

            log.info( "hive_context::executeDML commited: " + sql_ + "\nBinding\n--------\n" + bnd_.toString() );
        }
        catch ( SQLException ex )
        {
            log.error( "hive_context::executeDML exception: " + ex.getMessage() );

            try
            {
                con_.getConnection().rollback();
            }
            catch ( SQLException x ) 
            {
                log.error( "hive_context::executeDML rollback failed: " + x.getMessage() );
            }

            ok = false;

            log.info( "hive_context - DML rollback: " + sql_ + "\nBinding\n--------\n" + bnd_.toString() );
        }

        return ok;
    }

    //
    public boolean executeDDL() throws SQLException, hive_exception
    {
        boolean ok = false;

        //log.trace( "hive_context::executeDDL" );

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No DDL defined for hive context" );

        try
        {
            PreparedStatement stm = con_.getConnection().prepareStatement( sql_ );
            stm.executeUpdate();
            ok = true;

            log.info( "hive_context::execute ran: " + sql_ );
        }
        catch ( SQLException ex )
        {
            log.error( "hive_context::executeDML exception: " + ex.getMessage() );
            ok = false;
        }

        return ok;
    }

    //
    public ResultSetMetaData descSql() throws SQLException, hive_exception
    {
        ResultSetMetaData rmd = null;

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( rst_ == null )
        {
            //log.trace( "hive_context::descSql: " + sql_ );
            PreparedStatement stm = applyBindings( con_.getConnection().prepareStatement( limitSql( sql_ ) ) );
            ResultSet rst = stm.executeQuery();
            rmd = rst.getMetaData();
        }
        else
        {
            if ( setResultSetMetaData() )
                rmd = getResultSetMetaData();
        }

        return rmd;
    }

    // private functions ...

    //
    private boolean setPreparedStatement() throws SQLException, hive_exception
    {
        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( stm_ == null )
            stm_ = applyBindings( con_.getConnection().prepareStatement( sql_ ) );

        //log.trace( "hive_context::setPreparedStatement returns: " + ( ! ( stm_ == null ) ) );
        return ( ! ( stm_ == null ) );
    }

    //
    private boolean setResultSet() throws SQLException, hive_exception
    {
        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( rst_ == null )
        {
            if ( setPreparedStatement() )
                rst_ = stm_.executeQuery();
        }

        //log.trace( "hive_context::setResultSet returns: " + ( ! ( rst_ == null ) ) );
        return ( ! ( rst_ == null ) );
    }

    //
    private boolean setResultSetMetaData() throws SQLException, hive_exception
    {
        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( rmd_ == null )
        {
            if ( setResultSet() )
                rmd_ = rst_.getMetaData();
        }

        //log.trace( "hive_context::setResultSetMetaData returns: " + ( ! ( rmd_ == null ) ) );
        return ( ! ( rmd_ == null ) );
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

            if ( qry.substring( qry.length() ).equals( " " ) )
                qry += "limit 0";
            else
                qry += " limit 0";
        }

        //log.trace( "hive_context::limitSql (zero): " + qry );
        return qry;
    }

    //
    private static String limitSql( String sql, int rsz )
    {
        String qry = sql.trim();
        
        if ( qry.toLowerCase().indexOf( "select" ) == 0 )
        {
            Pattern ptn = Pattern.compile( "limit [0-9]" );

            if ( rsz < 0 )
                rsz = 0;

            if ( rsz > 0 )
            {
                while ( qry.indexOf( "  " ) >= 0 )
                    qry = qry.replace( "  ", " " );

                while ( qry.indexOf( "\n" ) >= 0 )
                    qry = qry.replace( "\n", " " );

                while ( qry.indexOf( "\r" ) >= 0 )
                    qry = qry.replace( "\r", " " );

                Matcher reg = ptn.matcher( qry.toLowerCase() );

                if ( reg.find() )
                {
                    int lim = 0;

                    lim = Integer.parseInt( qry.substring( reg.start() + 5 ).trim() );
                    //log.trace( "hive_context::limitSql extsing limit: " + Integer.toString( lim ) );

                    if ( lim > rsz )
                    {
                        qry = qry.substring( 0, reg.start() );

                        if ( qry.substring( qry.length() ).equals( " " ) )
                            qry += "limit " + Integer.toString( rsz );
                        else
                            qry += " limit " + Integer.toString( rsz );
                    }
                }
                else
                    qry += " limit " + Integer.toString( rsz );
            }
        }

        //log.trace( "hive_context::limitSql (" + Integer.toString( rsz ) + "): " + qry );
        return qry;
    }

    //
    public boolean equals( hive_context val )
    {
        boolean eq = false;

        if ( val != null )
        {
            if ( ( con_ != null )
              && ( bnd_ != null )
              && ( sql_ != null ) )
            {
                eq = ( ( con_.equals( val.con_ ) )
                    && ( bnd_.equals( val.bnd_ ) )
                    && ( sql_.equals( val.sql_ ) ) );
            }
            else
            {
                eq = true;

                if ( con_ != null )
                    eq = ( con_.equals( val.con_ ) && eq );

                if ( bnd_ != null )
                    eq = ( bnd_.equals( val.bnd_ ) && eq );

                if ( sql_ != null )
                    eq = ( sql_.equals( val.sql_ ) && eq );
            }
        }

        return eq;
    }

    //
    public boolean colRuleMapped( String col )
    {
        boolean ok = false;

        //log.trace( "hive_context::colRuleMapped: " + col + ", rule size: " + rul_.size() );

        if ( rul_.size() > 0 )
            ok = ( rul_.item( HINT_USE_MAP, col ) != null );

        //log.trace( "hive_context::colRuleMapped: returns: " + ( ( ok ) ? "true" : "false" ) );
        return ok;
    }

    //
    public boolean colRuleMapped( int idx ) throws SQLException, hive_exception
    {
        boolean ok = false;

        //log.trace( "hive_context::colRuleMapped index: " + idx );

        if ( rmd_ == null )
        {
            if ( rst_ == null )
                setResultSet();

            rmd_ = rst_.getMetaData();
        }

        ok = colRuleMapped( rmd_.getColumnName( idx ) );

        //log.trace( "hive_context::colRuleMapped returns: " + ( ( ok ) ? "true" : "false" ) );
        return ok;
    }

    //
    public int colRuleTypeCode( String col )
    {
        int val = 0;
        hive_hint dat = rul_.item( HINT_USE_MAP, col );

        if ( dat != null )
            val = dat.code;

        //log.trace( "hive_context::colRuleTypeCode: returns " + val + " [" + hive_types.to_typecode( val ) + "]" );
        return val;
    }

    //
    public int colRuleTypeJdbc( String col )
    {
        int val = 0;
        hive_hint dat = rul_.item( HINT_USE_MAP, col );

        if ( dat != null )
            val = dat.jdbc;

        //log.trace( "hive_context::colRuleTypeCode: returns " + val + " [" + hive_types.to_typecode( val ) + "]" );
        return val;
    }

    //
    public int colRuleLength( String col )
    {
        //log.trace( "hive_context::colRuleLength: " + col + " (redirecting)" );

        // length and precision are the same
        return colRulePrecision( col );
    }

    //
    public int colRulePrecision( String col )
    {
        int val = 0;
        hive_hint dat = rul_.item( HINT_USE_MAP, col );

        if ( dat != null )
        {
            if ( dat.size.length() > 0 )
            {
                try
                {
                    val = Integer.parseInt( dat.size );
                }
                catch ( NumberFormatException ex )
                {
                    val = hive_types.default_precision_typecode( dat.code );
                }
            }
            else
                val = hive_types.default_precision_typecode( dat.code );
        }
        else
            val = hive_types.default_precision_typecode( dat.code );

        return val;
    }

    //
    public int colRuleScale( String col )
    {
        int val = 0;
        hive_hint dat = rul_.item( HINT_USE_MAP, col );

        if ( dat != null )
        {
            if ( dat.opts.length() > 0 )
            {
                try
                {
                    val = Integer.parseInt( dat.opts );
                }
                catch ( NumberFormatException ex )
                {
                    val = hive_types.default_scale_typecode( dat.code );
                }
            }
            else
                val = hive_types.default_scale_typecode( dat.code );
        }
        else
            val = hive_types.default_scale_typecode( dat.code );

        return val;
    }

    //
    public hive_attribute colRuleAttr( hive_attribute atr )
    {
        switch ( atr.code )
        {
            case hive_types.TYPECODE_RAW:
            case hive_types.TYPECODE_CHAR:
            case hive_types.TYPECODE_VARCHAR2:
            case hive_types.TYPECODE_VARCHAR:
            case hive_types.TYPECODE_NCHAR:
            case hive_types.TYPECODE_NVARCHAR2:
                atr.prec  = -1;
                atr.scale = -1;
                break;

            case hive_types.TYPECODE_DATE:
                atr.len   = -1;
                atr.prec  = -1;
                atr.scale = -1;
                break;

            case hive_types.TYPECODE_TIMESTAMP:
            case hive_types.TYPECODE_TIMESTAMP_TZ:
            case hive_types.TYPECODE_TIMESTAMP_LTZ:
            case hive_types.TYPECODE_INTERVAL_YM:
            case hive_types.TYPECODE_INTERVAL_DS:
            case hive_types.TYPECODE_NUMBER:
            case hive_types.TYPECODE_BFLOAT:
                atr.len   = -1;
                break;

            case hive_types.TYPECODE_MLSLABEL:
            case hive_types.TYPECODE_BLOB:
            case hive_types.TYPECODE_BFILE:
            case hive_types.TYPECODE_CLOB:
            case hive_types.TYPECODE_CFILE:
            case hive_types.TYPECODE_REF:
            case hive_types.TYPECODE_OBJECT:
            case hive_types.TYPECODE_VARRAY:
            case hive_types.TYPECODE_TABLE:
            case hive_types.TYPECODE_NAMEDCOLLECTION:
            case hive_types.TYPECODE_OPAQUE:
            case hive_types.TYPECODE_NCLOB:
            case hive_types.TYPECODE_BDOUBLE:
            case hive_types.TYPECODE_UROWID:
            default:
                break;
        }

        return atr;
    }
};
