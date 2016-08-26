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
import java.util.regex.*;

import oracle.sql.*;
import oracle.jdbc.*;

//
@SuppressWarnings("deprecation")
public class hive_context
{
    //
    private hive_connection   con_;
    private hive_bindings     bnd_;

    //
    private String            sql_;
    private PreparedStatement stm_;
    private ResultSet         rst_;
    private ResultSetMetaData rmd_;

    //
    private long rec_;

    // ctor
    //
    public hive_context( String sql, oracle.sql.ARRAY bnd, oracle.sql.STRUCT con ) throws SQLException, hive_exception
    {
        log.trace( "hive_context ctor: " + sql );
        sql_ = sql;

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

    //
    public boolean ready()
    {
        log.trace( "hive_context ready: " + ( ! ( rst_ == null ) ) );
        return ( ! ( rst_ == null ) );
    }

    //
    public void clear()
    {
        log.trace( "hive_context clear" );
        con_ = null;
        sql_ = null;
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

                for ( hive_bind bnd : bnd_.binds )
                {
                    idx += 1;

                    switch ( bnd.type )
                    {
                        //
                        case hive_bind.TYPE_BOOL:

                            log.trace( "applyBindings: TYPE_BOOL" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setBoolean( idx, bnd.toBool() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.NUMBER );

                            break;

                        //
                        case hive_bind.TYPE_DATE:

                            log.trace( "applyBindings: TYPE_DATE" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setDate( idx, bnd.toDate() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.DATE );

                            break;

                        //
                        case hive_bind.TYPE_FLOAT:

                            log.trace( "applyBindings: TYPE_FLOAT" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setFloat( idx, bnd.toFloat() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.NUMBER );

                            break;

                        //
                        case hive_bind.TYPE_INT:

                            log.trace( "applyBindings: TYPE_INT" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setInt( idx, bnd.toInt() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.NUMBER );

                            break;

                        //
                        case hive_bind.TYPE_LONG:

                            log.trace( "applyBindings: TYPE_LONG" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setLong( idx, bnd.toLong() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.NUMBER );

                            break;

                        //
                        case hive_bind.TYPE_NULL:

                            log.trace( "applyBindings: TYPE_NULL" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setNull( idx, Types.VARCHAR );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Encountered OUT or INOUT scope for TYPE_NULL" );

                            break;

                        //
                        case hive_bind.TYPE_ROWID:

                            log.trace( "applyBindings: TYPE_ROWID" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setString( idx, bnd.toRowid() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.VARCHAR );

                            break;

                        //
                        case hive_bind.TYPE_SHORT:

                            log.trace( "applyBindings: TYPE_SHORT" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setShort( idx, bnd.toShort() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.NUMBER );

                            break;

                        //
                        case hive_bind.TYPE_STRING:

                            log.trace( "applyBindings: TYPE_STRING" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setString( idx, bnd.toVarchar() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.VARCHAR );

                            break;

                        //
                        case hive_bind.TYPE_TIME:

                            log.trace( "applyBindings: TYPE_TIME" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setTime( idx, bnd.toTime() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.DATE );

                            break;

                        //
                        case hive_bind.TYPE_TIMESTAMP:

                            log.trace( "applyBindings: TYPE_TIMESTAMP" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setTimestamp( idx, bnd.toTimestamp() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.TIMESTAMP );

                            break;

                        //
                        case hive_bind.TYPE_URL:

                            log.trace( "applyBindings: TYPE_URL" );

                            if ( ( bnd.scope == hive_bind.SCOPE_IN )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                stmt.setURL( idx, bnd.toUrl() );

                            if ( ( bnd.scope == hive_bind.SCOPE_OUT )
                              || ( bnd.scope == hive_bind.SCOPE_INOUT ) )
                                throw new hive_exception( "Out parameter are not currently supported" );
                                //stmt.registerOutParameter( idx, java.sql.Types.VARCHAR );

                            break;

                        //
                        default:
                            throw new hive_exception( "Unknown binding type [" + bnd.type + "] encountered" );
                    }
                }
            }
        }

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

        log.trace( "hive_context columnCount rmd_: " + rmd_ );
        return rmd_.getColumnCount();
    }

    //
    public int columnType( int i ) throws SQLException, hive_exception
    {
        if ( rmd_ == null )
        {
            if ( rst_ == null )
                setResultSet();

            rmd_ = rst_.getMetaData();
        }

        log.trace( "hive_context columnType rmd_: " + rmd_ );
        return rmd_.getColumnType( i );
    }

    // recordset
    //
    public boolean next() throws SQLException
    {
        ++rec_;

        log.trace( "hive_context next rst_: " + rst_ );
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

        log.trace( "hive_context execute" );

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No SQL defined for hive context" );

        if ( limit != null )
        {
            log.trace( "execute query_limit: " + limit );

            try
            {
                sql_ = limitSql( sql_, Integer.parseInt( limit.trim() ) );
            }
            catch ( NumberFormatException ex )
            {
                log.error( "execute NumberFormatException: " + ex.getMessage() );
                // ... do nothing
            }
        }

        return setResultSet();
    }

    //
    public boolean executeDML() throws SQLException, hive_exception
    {
        boolean ok = false;

        log.trace( "hive_context executeDML" );

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No DML defined for hive context" );

        try
        {
            PreparedStatement stm = applyBindings( con_.getConnection().prepareStatement( sql_ ) );
            stm.executeUpdate();
            con_.getConnection().commit();
            ok = true;

            log.info( "DML commited: " + sql_ + "\nBinding\n--------" + bnd_.toString() );
        }
        catch ( SQLException ex )
        {
            log.error( "executeDML exception: " + ex.getMessage() );

            try
            {
                con_.getConnection().rollback();
            }
            catch ( SQLException x ) 
            {
                log.error( "executeDML rollbac failed: " + x.getMessage() );
            }

            ok = false;

            log.info( "DML rollback: " + sql_ + "\nBinding\n--------" + bnd_.toString() );
        }

        return ok;
    }

    //
    public boolean executeDDL() throws SQLException, hive_exception
    {
        boolean ok = false;

        log.trace( "hive_context executeDDL" );

        if ( ( sql_ == null ) || ( sql_.length() == 0 ) )
            throw new hive_exception( "No DDL defined for hive context" );

        try
        {
            PreparedStatement stm = con_.getConnection().prepareStatement( sql_ );
            stm.executeUpdate();
            ok = true;

            log.info( "DDL executed: " + sql_ );
        }
        catch ( SQLException ex )
        {
            log.error( "executeDML exception: " + ex.getMessage() );
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
            log.trace( "describing sql: " + sql_ );
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

        log.trace( "hive_context setPreparedStatement returns: " + ( ! ( stm_ == null ) ) );
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

        log.trace( "hive_context setResultSet returns: " + ( ! ( rst_ == null ) ) );
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

        log.trace( "hive_context setResultSetMetaData returns: " + ( ! ( rmd_ == null ) ) );
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

        log.trace( "limitSql (zero): " + qry );
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
                    log.trace( "limitSql extsing limit: " + Integer.toString( lim ) );

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

        log.trace( "limitSql (" + Integer.toString( rsz ) + "): " + qry );
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
};

