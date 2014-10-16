package com.github.ambry.messageformat;

/**
 * The error codes that the message format package returns
 */
public enum MessageFormatErrorCodes {
  Data_Corrupt,
  Header_Constraint_Error,
  Unknown_Format_Version,
  Store_Key_Id_MisMatch,
  IO_Error
}
