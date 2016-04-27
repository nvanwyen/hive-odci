// from: http://dbprism.cvs.sourceforge.net/viewvc/dbprism/odi/src/java/org/apache/lucene/search/highlight/ojvm/Highlighter.java?revision=1.2&content-type=text%2Fplain

package org.apache.lucene.search.highlight.ojvm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import java.math.BigDecimal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.CLOB;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.indexer.ContextManager;
import org.apache.lucene.indexer.DefaultUserDataStore;
import org.apache.lucene.indexer.Entry;
import org.apache.lucene.indexer.LuceneDomainIndex;
import org.apache.lucene.indexer.Parameters;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.store.OJVMDirectory;
import org.apache.lucene.store.OJVMUtil;
import org.apache.lucene.util.Version;


/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// implementation type

public class Highlighter {
  /**
   * Java Util Logging variables and default values
   */
  private static Logger logger = null;

  /**
   * Constant used to get Logger name
   */
  static final String CLASS_NAME = Highlighter.class.getName();

  static {
    logger = Logger.getLogger(CLASS_NAME);
    // default Log level, override it using LuceneDomainIndex.setLogLevel('level')
    logger.setLevel(Level.WARNING);
  }

  final static BigDecimal SUCCESS = new BigDecimal(0);
  final static BigDecimal ERROR = new BigDecimal(1);

  static public StoredCtx CreateContext(Connection conn, String indexName,
                                       String qryString, String cols,
                                       ResultSet rset) throws SQLException,
                                                              IOException,
                                                              ParseException {
    logger.entering(CLASS_NAME, "CreateContext",
                    new Object[] { indexName, qryString, cols,
                                   rset });
    Entry entry = OJVMDirectory.getCachedDirectory(indexName);
    Parameters pars = entry.getDirectory().getParameters();
    String logLevel = pars.getParameter("LogLevel","WARNING");
    logger.setLevel(Level.parse(logLevel)); // Restore log level
    ResultSetMetaData mtdt = rset.getMetaData();
    int colsCount = mtdt.getColumnCount();
    String[] colsName = new String[colsCount];
    String[] colsToHighLight = cols.split(",");
    if (colsToHighLight.length == 0)
      throw new SQLException("highlight(): can not work with 0 length columns to highlight array");
    int[] colsType = new int[colsCount];
    boolean[] highlightedCols = new boolean[colsCount];
    for (int i = 0; i < colsCount; i++) {
      colsName[i] = mtdt.getColumnName(i + 1);
      colsType[i] = mtdt.getColumnType(i + 1);
      for (int j = 0; j < colsToHighLight.length; j++)
        if (colsName[i].equals(colsToHighLight[j]))
          highlightedCols[i] = true;
      //logger.info("colsName[" + i + "]=" + colsName[i] + " colsType[" + i +
      //            "]=" + colsType[i]);
    }
    String columnName = pars.getParameter("DefaultColumn");
    logger.info("DefaultColumn: " + columnName);
    QueryParser parser = entry.getParser();
    Query qry = parser.parse(qryString);
    Query qryRewrited = qry.rewrite(entry.getReader());
    Formatter formatter = LuceneDomainIndex.getFormatter(pars);
    org.apache.lucene.search.highlight.Highlighter highlighter =
      new org.apache.lucene.search.highlight.Highlighter(formatter,
                                                         new QueryScorer(qryRewrited));
    int fragmentSize = Integer.parseInt(pars.getParameter("FragmentSize","100"));
    highlighter.setTextFragmenter(new SimpleFragmenter(fragmentSize));
    int maxNumFragmentsRequired = Integer.parseInt(pars.getParameter("MaxNumFragmentsRequired","4"));
    String fragmentSeparator = pars.getParameter("FragmentSeparator","...");

    StoredCtx ctx =
      new StoredCtx(conn, indexName, columnName, highlightedCols, rset,
                    colsName, colsType, highlighter, entry);
    ctx.setStartTime(System.currentTimeMillis());
    ctx.setMaxNumFragmentsRequired(maxNumFragmentsRequired);
    ctx.setFragmentSeparator(fragmentSeparator);
    logger.exiting(CLASS_NAME, "CreateContext", ctx);
    return ctx;
  }
  
  static public BigDecimal ODCITableStart(BigDecimal[] sctx, String indexName,
                                          String qryString, String cols,
                                          ResultSet rset) throws SQLException,
                                                              IOException,
                                                              ParseException {
    logger.entering(CLASS_NAME, "ODCITableStart",
                    new Object[] { sctx[0], indexName, qryString, cols,
                                   rset });
    Connection conn = OJVMUtil.getConnection();
    StoredCtx ctx =
      CreateContext(conn, indexName, qryString, cols , rset);
    // register stored context with cartridge services
    int key;
    key = ContextManager.setContext(ctx);

    // create a Highlighter instance and store the key in it
    sctx[0] = new BigDecimal(key);
    logger.info("key '" + sctx[0] + "'");
    logger.exiting(CLASS_NAME, "ODCITableStart", SUCCESS);
    return SUCCESS;
  };

  static public BigDecimal ODCITableStart(BigDecimal[] sctx, String indexName,
                                          String qryString, String cols,
                                          String stmt) throws SQLException,
                                                              IOException,
                                                              ParseException {
    logger.entering(CLASS_NAME, "ODCITableStart",
                    new Object[] { sctx[0], indexName, qryString, cols,
                                   stmt });
    Connection conn = OJVMUtil.getConnection();
    PreparedStatement st = conn.prepareStatement(stmt);
    ResultSet rset = st.executeQuery();
    StoredCtx ctx =
      CreateContext(conn, indexName, qryString, cols , rset);
    // Store the statment to be free at ODCIClose
    ctx.setSt(st);
    // register stored context with cartridge services
    int key;
    key = ContextManager.setContext(ctx);

    // create a Highlighter instance and store the key in it
    sctx[0] = new BigDecimal(key);
    logger.info("key '" + sctx[0] + "'");
    logger.exiting(CLASS_NAME, "ODCITableStart", SUCCESS);
    return SUCCESS;
  }

  static public BigDecimal ODCITableFetch(BigDecimal ctx, BigDecimal nrows,
                                          java.sql.Array[] outSet) throws SQLException,
                                                                          InvalidTokenOffsetsException,
                                                                          IOException {
    //logger.entering(CLASS_NAME, "ODCITableFetch",
    //                new Object[] { ctx, nrows, outSet });
    // retrieve stored context using the key
    StoredCtx sctx;
    sctx = (StoredCtx)ContextManager.getContext(ctx.intValue());
    Entry entry = sctx.getEntry();
    Connection conn = sctx.getConnection();
    ResultSet rset = sctx.getRset();
    boolean[] highlightedCols = sctx.getHighlightedCols();
    Analyzer analyzer = entry.getAnalyzer();
    org.apache.lucene.search.highlight.Highlighter highlighter =
      sctx.getHighlighter();
    String fragmentSeparator = sctx.getFragmentSeparator();
    int maxNumFragmentsRequired = sctx.getMaxNumFragmentsRequired();
    if (rset.next()) {
      String[] cols = sctx.getCols();
      int[] types = sctx.getTypes();
      int numCols = cols.length;
      Object[] vals = new Object[numCols];
      for (int i = 0; i < numCols; i++) {
        Object val = rset.getObject(i + 1);
        if (val instanceof String && highlightedCols[i]) {
          // supported highlight columns
          val =
              highlighter.getBestFragments(analyzer.tokenStream(sctx.getColumnName(),
                                                                new StringReader((String)val)),
                                           (String)val, maxNumFragmentsRequired, fragmentSeparator);
        } else if (val instanceof CLOB && highlightedCols[i]) {
          // supported highlight columns
          CLOB hval = CLOB.createTemporary(conn, true, CLOB.DURATION_CALL);
          if (((CLOB)val).getLength() > 0) {
            String text =
              DefaultUserDataStore.readStream(new BufferedReader(((CLOB)val).characterStreamValue()));
            text =
                highlighter.getBestFragments(analyzer.tokenStream(sctx.getColumnName(),
                                                                  new StringReader(text)),
                                             text, maxNumFragmentsRequired, fragmentSeparator);
            hval.setString(1L, text);
          }
          val = hval;
        }
        //logger.info("fetch["+i+"] val: "+val+" class: "+val.getClass() + " is highlighted: " + highlightedCols[i]);
        int jdbcType = (val instanceof Timestamp) ? 91 : types[i];
        Object[] attrs = // RT_FETCH_ATTRIBUTES
          { new BigDecimal(jdbcType), 
          (jdbcType == 12 ) ? val : null, // V2_COLUMN
          (jdbcType == 2 ) ? val : null, // NUM_COLUMN
          (jdbcType == 91 ) ? val : null, // DATE_COLUMN
          (jdbcType == 2005 ) ? val : null, // CLOB_COLUMN
          null, // RAW_COLUMN
          null, // RAW_ERROR
          null, // RAW_LENGTH
          null, // IDS_COLUMN
          null, // IYM_COLUMN
          (jdbcType == 93 ) ? val : null, // TS_COLUMN
          (jdbcType == -101 ) ? val : null, // TSTZ_COLUMN
          (jdbcType == -102) ? val : null, // TSLTZ_COLUMN
          new Integer(0), // CVL_OFFSET
          null // CVL_LENGTH
          } ;
        StructDescriptor sd =
          new StructDescriptor("LUCENE.RT_FETCH_ATTRIBUTES", conn);
        //logger.entering(CLASS_NAME, "ODCITableFetch.fetch["+i+"]",
        //                attrs);
        vals[i] = new STRUCT(sd, conn, attrs);
      }
      // get the nrows parameter, but return up to 10 rows
      // return if no rows found
      ArrayDescriptor arraydesc =
        ArrayDescriptor.createDescriptor("LUCENE.ROWINFO", conn);
      ARRAY arr = new ARRAY(arraydesc, conn, vals);
      outSet[0] = arr;
    } else // end of fetch
      outSet[0] = null;
    //logger.exiting(CLASS_NAME, "ODCITableFetch", SUCCESS);
    return SUCCESS;
  }

  static public BigDecimal ODCITableClose(BigDecimal ctx) throws SQLException {

    // retrieve stored context using the key, and remove from ContextManager
    logger.entering(CLASS_NAME, "ODCITableClose", new Object[] { ctx });
    StoredCtx sctx;
    int key = ctx.intValue();
    sctx = (StoredCtx)ContextManager.clearContext(key);
    long elapsedTime = System.currentTimeMillis() - sctx.getStartTime();
    logger.info("Elapsed time: "+elapsedTime+" millisecond.");
    // close the result set
    // close the statement
    OJVMUtil.closeDbResources(sctx.getSt(),sctx.getRset());
    logger.exiting(CLASS_NAME, "ODCITableClose", SUCCESS);
    return SUCCESS;
  }
}
