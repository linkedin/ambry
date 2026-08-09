/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.utils;

import java.nio.channels.ClosedChannelException;


/**
 * A {@link ClosedChannelException} thrown specifically when a channel is confirmed, with high confidence, to have
 * been closed because of a client-rooted termination (e.g. the client disconnected, reset the connection, or went
 * idle for longer than the configured timeout).
 * <p/>
 * Extending {@link ClosedChannelException} keeps this backward compatible with any existing code that catches or
 * checks for {@link ClosedChannelException}. Callers that need to distinguish a client-rooted termination from any
 * other reason a channel might close can do so via an {@code instanceof ClientChannelCloseException} check, or via
 * {@link Utils#isPossibleClientTermination(Throwable)}.
 */
public class ClientChannelCloseException extends ClosedChannelException {
}
