package com.github.ambry.messageformat;

public class MessageFormatException extends Exception {
  private final MessageFormatErrorCodes error;

  public MessageFormatException(String message, MessageFormatErrorCodes error) {
    super(message);
    this.error = error;
  }

  public MessageFormatException(String message, Throwable e, MessageFormatErrorCodes error) {
    super(message, e);
    this.error = error;
  }

  public MessageFormatException(Throwable e, MessageFormatErrorCodes error) {
    super(e);
    this.error = error;
  }

  public MessageFormatErrorCodes getErrorCode() {
    return this.error;
  }
}
