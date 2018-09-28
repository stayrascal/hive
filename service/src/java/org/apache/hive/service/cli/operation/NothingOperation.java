package org.apache.hive.service.cli.operation;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

public class NothingOperation extends MetadataOperation {
  private TableSchema resultSchema = null;
  private Driver driver = null;
  private Schema mResultSchema = null;
  private SerDe serde = null;
  private String statement;
  private boolean fetchStarted = false;
  private transient final List<Object> convey = new ArrayList<Object>();

  public NothingOperation(HiveSession parentSession, String statement) {
    super(parentSession, OperationType.NOTHING);
    this.statement = statement;
  }

  @Override
  protected void runInternal() throws HiveSQLException {
    final HiveConf opConfig = getConfigForOperation();
    prepare(opConfig);
  }

  private void prepare(HiveConf hiveConf) throws HiveSQLException {
    try {
      driver = new Driver(hiveConf, getParentSession().getUserName());
      String guid64 = Base64.encodeBase64URLSafeString(getHandle().getHandleIdentifier()
          .toTHandleIdentifier().getGuid()).trim();
      driver.setOperationId(guid64);
      driver.setTryCount(Integer.MAX_VALUE);

      String subStatement = new VariableSubstitution().substitute(hiveConf, statement);
      CommandProcessorResponse response = driver.compileAndRespond(subStatement);
      if (0 != response.getResponseCode()) {
        throw toSQLException("Error while compiling statement", response);
      }

      mResultSchema = driver.getSchema();

      // hasResultSet should be true only if the query has a FetchTask
      // "explain" is an exception for now
      if (driver.getPlan().getFetchTask() != null) {
        //Schema has to be set
        if (mResultSchema == null || !mResultSchema.isSetFieldSchemas()) {
          throw new HiveSQLException("Error compiling query: Schema and FieldSchema " +
              "should be set when query plan has a FetchTask");
        }
        resultSchema = new TableSchema(mResultSchema);
        setHasResultSet(true);
      } else {
        setHasResultSet(false);
      }
      // Set hasResultSet true if the plan has ExplainTask
      // TODO explain should use a FetchTask for reading
      for (Task<? extends Serializable> task : driver.getPlan().getRootTasks()) {
        if (task.getClass() == ExplainTask.class) {
          resultSchema = new TableSchema(mResultSchema);
          setHasResultSet(true);
          break;
        }
      }
    } catch (HiveSQLException e) {
      setState(OperationState.ERROR);
      throw e;
    }
  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    if (resultSchema == null) {
      resultSchema = new TableSchema(driver.getSchema());
    }
    return resultSchema;
  }


  public void setResultPath(String resultPath) {
    driver.setResultPath(resultPath);
  }

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    validateDefaultFetchOrientation(orientation);
    RowSet rowSet = RowSetFactory.create(resultSchema, getProtocolVersion());

    try {
      /* if client is requesting fetch-from-start and its not the first time reading from this operation
       * then reset the fetch position to beginning
       */
      if (orientation.equals(FetchOrientation.FETCH_FIRST) && fetchStarted) {
        driver.resetFetch();
      }
      fetchStarted = true;
      driver.setMaxRows((int) maxRows);
      if (driver.getResults(convey)) {
        return decode(convey, rowSet);
      }
      return rowSet;
    } catch (IOException e) {
      throw new HiveSQLException(e);
    } catch (CommandNeedRetryException e) {
      throw new HiveSQLException(e);
    } catch (Exception e) {
      throw new HiveSQLException(e);
    } finally {
      convey.clear();
    }
  }

  private RowSet decode(List<Object> rows, RowSet rowSet) throws Exception {
    if (driver.isFetchingTable()) {
      return prepareFromRow(rows, rowSet);
    }
    return decodeFromString(rows, rowSet);
  }

  // already encoded to thrift-able object in ThriftFormatter
  private RowSet prepareFromRow(List<Object> rows, RowSet rowSet) throws Exception {
    for (Object row : rows) {
      rowSet.addRow((Object[]) row);
    }
    return rowSet;
  }

  private RowSet decodeFromString(List<Object> rows, RowSet rowSet)
      throws SQLException, SerDeException {
    getSerDe();
    StructObjectInspector soi = (StructObjectInspector) serde.getObjectInspector();
    List<? extends StructField> fieldRefs = soi.getAllStructFieldRefs();

    Object[] deserializedFields = new Object[fieldRefs.size()];
    Object rowObj;
    ObjectInspector fieldOI;

    int protocol = getProtocolVersion().getValue();
    for (Object rowString : rows) {
      try {
        rowObj = serde.deserialize(new BytesWritable(((String) rowString).getBytes("UTF-8")));
      } catch (UnsupportedEncodingException e) {
        throw new SerDeException(e);
      }
      for (int i = 0; i < fieldRefs.size(); i++) {
        StructField fieldRef = fieldRefs.get(i);
        fieldOI = fieldRef.getFieldObjectInspector();
        Object fieldData = soi.getStructFieldData(rowObj, fieldRef);
        deserializedFields[i] = SerDeUtils.toThriftPayload(fieldData, fieldOI, protocol);
      }
      rowSet.addRow(deserializedFields);
    }
    return rowSet;
  }

  private HiveConf getConfigForOperation() {
    HiveConf sqlOperationConf = getParentSession().getHiveConf();
    return sqlOperationConf;
  }

  private SerDe getSerDe() throws SQLException {
    if (serde != null) {
      return serde;
    }
    try {
      List<FieldSchema> fieldSchemas = mResultSchema.getFieldSchemas();
      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      if (fieldSchemas != null && !fieldSchemas.isEmpty()) {
        for (int pos = 0; pos < fieldSchemas.size(); pos++) {
          if (pos != 0) {
            namesSb.append(",");
            typesSb.append(",");
          }
          namesSb.append(fieldSchemas.get(pos).getName());
          typesSb.append(fieldSchemas.get(pos).getType());
        }
      }
      String names = namesSb.toString();
      String types = typesSb.toString();

      serde = new LazySimpleSerDe();
      Properties props = new Properties();
      if (names.length() > 0) {
        LOG.debug("Column names: " + names);
        props.setProperty(serdeConstants.LIST_COLUMNS, names);
      }
      if (types.length() > 0) {
        LOG.debug("Column types: " + types);
        props.setProperty(serdeConstants.LIST_COLUMN_TYPES, types);
      }
      SerDeUtils.initializeSerDe(serde, new HiveConf(), props, null);

    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
    return serde;
  }

  public void setTblDir(String tblDir) {
    driver.setTblDir(tblDir);
  }
}
