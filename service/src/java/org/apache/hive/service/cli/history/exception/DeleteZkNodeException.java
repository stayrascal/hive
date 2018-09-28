package org.apache.hive.service.cli.history.exception;

public class DeleteZkNodeException extends RuntimeException {
  public DeleteZkNodeException(String message) {
    super(message);
  }

  public DeleteZkNodeException(String message, Throwable cause) {
    super(message, cause);
  }
}
