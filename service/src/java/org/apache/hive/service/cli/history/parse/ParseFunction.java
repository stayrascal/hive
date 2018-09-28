package org.apache.hive.service.cli.history.parse;

public interface ParseFunction<T> {
  T parse(byte[] bytes);
}
