package org.apache.hive.service.cli.history;

public class ExecuteRecord {
  private String sql;
  private String appId;
  private String queryId;
  private ExecuteStatus status;
  private String retUrl;
  private Long startTime;
  private Long endTime;
  private String operationId;
  private String hostName;

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public ExecuteStatus getStatus() {
    return status;
  }

  public void setStatus(ExecuteStatus status) {
    this.status = status;
  }

  public String getRetUrl() {
    return retUrl;
  }

  public void setRetUrl(String retUrl) {
    this.retUrl = retUrl;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  public String getOperationId() {
    return operationId;
  }

  public void setOperationId(String operationId) {
    this.operationId = operationId;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }
}
