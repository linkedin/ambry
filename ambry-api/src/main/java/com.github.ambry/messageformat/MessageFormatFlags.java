package com.github.ambry.messageformat;

// TODO: MessageFormatFlags is an obtuse name. Is there a better name? The Javadoc does not help convey what a
// Message, MessageFormat, or MessageFormatFlag is.
/**
 * Set of flags used to identify different types of messages
 */
public enum MessageFormatFlags {
  BlobProperties,
  BlobUserMetadata,
  // TODO: Add Info == BlobProperties + BlobUserMetadata
  Blob,
  All
}

