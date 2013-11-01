package com.github.ambry.messageformat;

/**
 * Set of flags used to identify different types of messages
 **/
public enum MessageFormatFlags {
  SystemMetadata,
  UserMetadata,
  MessageHeader,
  Data,
  All
}
