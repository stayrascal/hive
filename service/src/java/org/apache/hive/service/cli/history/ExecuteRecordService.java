package org.apache.hive.service.cli.history;


public interface ExecuteRecordService {
  ExecuteRecord createRecordNode(String sql);

  ExecuteRecord updateRecordNode(ExecuteRecord record);

  ExecuteRecord getExecuteRecordBySql(String sql);

  ExecuteRecord getExecuteRecordByMD5Sql(String md5Sql);

  String getSqlByOperationId(String operationId);

  ExecuteRecord getRecordByOperationId(String operationId);

  void deleteRecordNode(String sqlId);

  void createOperationNode(ExecuteRecord record);

  boolean isOriginalServerRestartedOrRemoved(ExecuteRecord record);

  void deleteOperationNode(String originalMD5OperationId);
}
