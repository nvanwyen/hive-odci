// from: http://dbprism.cvs.sourceforge.net/viewvc/dbprism/odi/src/java/org/apache/lucene/search/highlight/ojvm/StoredCtx.java?content-type=text%2Fplain
package org.apache.lucene.search.highlight.ojvm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.lucene.indexer.Entry;
import org.apache.lucene.search.highlight.Highlighter;

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

public class StoredCtx {
  private Entry entry;
  private org.apache.lucene.search.highlight.Highlighter highlighter;
  private String indexName;
  private String columnName;
  private boolean[] highlightedCols;
  private PreparedStatement st;
  private ResultSet rset;
  private String[] cols;
  private int[] types;
  private Connection connection;
  private long startTime = 0;
  int maxNumFragmentsRequired = 4;
  String fragmentSeparator = "...";

  public StoredCtx(Connection conn, String i, String cl, boolean[] c,
                   ResultSet rs, String[] cls, int[] tps,
                   org.apache.lucene.search.highlight.Highlighter h,Entry e) {
    connection = conn;
    indexName = i;
    columnName = cl;
    highlightedCols = c;
    st = null;
    rset = rs;
    cols = cls;
    types = tps;
    highlighter = h;
    entry = e;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setCols(String[] cols) {
    this.cols = cols;
  }

  public String[] getCols() {
    return cols;
  }

  public void setRset(ResultSet rset) {
    this.rset = rset;
  }

  public ResultSet getRset() {
    return rset;
  }

  public void setHighlightedCols(boolean[] highlightedCols) {
    this.highlightedCols = highlightedCols;
  }

  public boolean[] getHighlightedCols() {
    return highlightedCols;
  }

  public void setTypes(int[] types) {
    this.types = types;
  }

  public int[] getTypes() {
    return types;
  }

  public void setSt(PreparedStatement st) {
    this.st = st;
  }

  public PreparedStatement getSt() {
    return st;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public Connection getConnection() {
    return connection;
  }

  public void setHighlighter(Highlighter highlighter) {
    this.highlighter = highlighter;
  }

  public Highlighter getHighlighter() {
    return highlighter;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setEntry(Entry entry) {
    this.entry = entry;
  }

  public Entry getEntry() {
    return entry;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setMaxNumFragmentsRequired(int maxNumFragmentsRequired) {
    this.maxNumFragmentsRequired = maxNumFragmentsRequired;
  }

  public int getMaxNumFragmentsRequired() {
    return maxNumFragmentsRequired;
  }

  public void setFragmentSeparator(String fragmentSeparator) {
    this.fragmentSeparator = fragmentSeparator;
  }

  public String getFragmentSeparator() {
    return fragmentSeparator;
  }
}
