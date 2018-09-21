package org.apache.hive.service.cli.operation;

import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

public class NothingOperation extends MetadataOperation {

  public NothingOperation(HiveSession parentSession) {
    super(parentSession, OperationType.NOTHING);
  }

  @Override
  protected void runInternal() throws HiveSQLException {

  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    return null;
  }

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    return null;
  }
}
