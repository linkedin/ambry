/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.store;

/**
 * The error codes that the store returns
 */
public enum StoreErrorCodes {
  ID_Not_Found,
  TTL_Expired,
  ID_Deleted,
  IOError,
  Initialization_Error,
  Already_Exist,
  Store_Not_Started,
  Store_Already_Started,
  Store_Shutting_Down,
  Illegal_Index_Operation,
  Illegal_Index_State,
  Index_Creation_Failure,
  Index_Version_Error,
  Authorization_Failure,
  Unknown_Error,
  Already_Updated,
  Update_Not_Allowed,
  File_Not_Found,
  Channel_Closed,
  Life_Version_Conflict,
  ID_Not_Deleted,
  ID_Undeleted,
  ID_Deleted_Permanently
}
