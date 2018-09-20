package org.apache.hive.service.cli.history;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hive.service.cli.OperationState;

import java.nio.charset.Charset;

public class ExecuteRecord {
  private String sql;
  private String appId;
  private String queryId;
  private OperationState status;
  private String retUrl;
  private Long startTime;
  private Long endTime;

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

  public OperationState getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = OperationState.valueOf(status);
  }

  public void setStatus(OperationState status) {
    this.status = status;
  }

  public String getRetUrl() {
    return retUrl;
  }

  public void setRetUrl(String retUrl) {
    this.retUrl = retUrl;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public void setStartTime(String startTime) {
    if (!"null".equals(endTime) && !StringUtils.isEmpty(startTime)) {
      this.startTime = Long.parseLong(startTime);
    }
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  public void setEndTime(String endTime) {
    if (!"null".equals(endTime) && !StringUtils.isEmpty(endTime)) {
      this.endTime = Long.parseLong(endTime);
    }
  }

  @Override
  public String toString() {
    return "sql=" + sql +
            ",queryId=" + queryId +
            ",appId=" + appId +
            ",status=" + status.name() +
            ",retUrl=" + retUrl +
            ",startTime=" + startTime +
            ",endTime=" + endTime;
  }

  public byte[] getNodeData() {
    return toString().getBytes(Charset.forName("UTF-8"));
  }

  public static void main(String[] args) {
    ExecuteRecord record = new ExecuteRecord();
    record.setSql(DigestUtils.md5Hex("select sum(*) from test").toUpperCase());
    System.out.println(record);
  }
}
