package org.apache.hive.service.cli.history;

import org.apache.hive.service.cli.OperationState;

public enum ExecuteStatus {
  COMPILING(OperationState.INITIALIZED),
  RUNNING(OperationState.RUNNING),
  FINISHED(OperationState.FINISHED);

  private OperationState operationState;

  ExecuteStatus(OperationState state) {
    this.operationState = state;
  }

  public OperationState toOperationState() {
    return operationState;
  }
}
