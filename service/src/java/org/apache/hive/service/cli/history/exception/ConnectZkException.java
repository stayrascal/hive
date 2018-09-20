package org.apache.hive.service.cli.history.exception;

public class ConnectZkException extends RuntimeException {
  public ConnectZkException(String message) {
    super(message);
  }

  public ConnectZkException(String message, Throwable cause) {
    super(message, cause);
  }
}
