package org.apache.hive.service.cli.history;


import java.util.Optional;

public interface ExecuteRecordService {
  ExecuteRecord createRecordNode(String sql);

  ExecuteRecord updateRecordNode(ExecuteRecord record);

  Optional<ExecuteRecord> getExecuteRecordBySql(String sql);

  Optional<String> getSqlByOperationId(String operationId);

  Optional<ExecuteRecord> getRecordByOperationId(String operationId);

  void deleteRecordNode(String sqlId);

  void createOperationNode(ExecuteRecord record);

  boolean isOriginalServerRestarted(ExecuteRecord record);
}
