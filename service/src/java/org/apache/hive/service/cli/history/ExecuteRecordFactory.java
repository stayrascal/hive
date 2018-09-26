package org.apache.hive.service.cli.history;

import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hive.service.cli.history.exception.ParseException;
import org.codehaus.jackson.map.ObjectMapper;

public class ExecuteRecordFactory {
  public static ExecuteRecord buildNewRecord(String statement) {
    ExecuteRecord executeRecord = new ExecuteRecord();
    executeRecord.setSql(DigestUtils.md5Hex(statement).toUpperCase());
    executeRecord.setStatus(ExecuteStatus.COMPILING);
    executeRecord.setStartTime(System.currentTimeMillis());
    return executeRecord;
  }

  public static ExecuteRecord convertByteToRecord(byte[] bytes) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(bytes, ExecuteRecord.class);
    } catch (IOException e) {
      throw new ParseException("Cannot parse execute record: " + bytes);
    }
  }

  public static byte[] convertRecordToBytes(ExecuteRecord record) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsBytes(record);
    } catch (IOException e) {
      throw new ParseException("Cannot parse execute record: " + record);
    }
  }
}
