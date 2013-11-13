package com.github.ambry.messageformat;

/**
 * Used when the message format of the messages is not in a known format
 */
public class UnknownMessageFormatException extends Exception {

  public UnknownMessageFormatException(String message)
  {
    super(message);
  }

  public UnknownMessageFormatException(String message, Throwable e)
  {
    super(message,e);
  }

  public UnknownMessageFormatException(Throwable e)
  {
    super(e);
  }
}
