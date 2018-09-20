package org.apache.hive.service.cli.history;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.history.exception.ParseException;

import java.nio.charset.Charset;

public class ExecuteRecordFactory {
  public static ExecuteRecord buildNewRecord(String statement) {
    ExecuteRecord executeRecord = new ExecuteRecord();
    executeRecord.setSql(DigestUtils.md5Hex(statement).toUpperCase());
    executeRecord.setStatus(OperationState.INITIALIZED);
    executeRecord.setStartTime(System.currentTimeMillis());
    return executeRecord;
  }

  public static ExecuteRecord parseRecordFromStrBytes(byte[] data) {
    String recordStr = new String(data, Charset.forName("UTF-8"));
    String[] fields = recordStr.split(",");
    if (fields.length != 7) {
      throw new ParseException("Cannot parse execute record: " + recordStr);
    }
    ExecuteRecord record = new ExecuteRecord();
    record.setSql(fields[0].split("=")[1]);
    record.setAppId(fields[1].split("=")[1]);
    record.setQueryId(fields[2].split("=")[1]);
    record.setStatus(fields[3].split("=")[1]);
    record.setRetUrl(fields[4].split("=")[1]);
    record.setStartTime(fields[5].split("=")[1]);
    record.setEndTime(fields[6].split("=")[1]);
    return record;
  }
}
