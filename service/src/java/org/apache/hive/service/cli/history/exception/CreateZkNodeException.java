package org.apache.hive.service.cli.history.exception;

public class CreateZkNodeException extends RuntimeException {
  public CreateZkNodeException(String message) {
    super(message);
  }

  public CreateZkNodeException(String message, Throwable cause) {
    super(message, cause);
  }
}
