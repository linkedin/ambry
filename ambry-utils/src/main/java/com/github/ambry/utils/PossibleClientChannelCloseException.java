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
 * A {@link ClosedChannelException} thrown when a channel closed in a way that is <em>plausibly</em> client-rooted,
 * but where an equally plausible, well-understood non-client cause exists, so client-rooted termination cannot be
 * confirmed with the same confidence as {@link ClientChannelCloseException}. This is the "possible" tier of
 * client-termination classification, sitting between the "sure" tier ({@link ClientChannelCloseException}) and an
 * unclassified {@link ClosedChannelException} (e.g. a confirmed internal/server-side error).
 * <p/>
 * Known triggers for this exception, and their non-client alternate explanation:
 * <ul>
 *   <li>Idle timeout: a slow/backpressured destination write can leave a connection silent in both directions for
 *   the idle window even though the client itself is healthy - see the idle-timeout handling in
 *   {@code NettyMessageProcessor#userEventTriggered} for details.</li>
 *   <li>An {@link java.io.IOException} reaching Netty's {@code exceptionCaught} for an in-flight request: this is
 *   usually a client-facing socket error (e.g. connection reset while reading further content), but a destination
 *   write failure could in principle propagate here via Netty's implicit exception routing, so exclusivity is not
 *   proven.</li>
 * </ul>
 * <p/>
 * This is intentionally a sibling of {@link ClientChannelCloseException}, not a subtype of it, so a caller checking
 * {@code instanceof ClientChannelCloseException} for the high-confidence "sure" tier is never accidentally satisfied
 * by a "possible" tier exception. Both extend {@link ClosedChannelException} so any existing code that catches or
 * checks for {@link ClosedChannelException} keeps working. Callers can detect this tier via
 * {@code instanceof PossibleClientChannelCloseException}, or via {@link Utils#isPossibleClientTermination(Throwable)}
 * (which also returns {@code true} for the "sure" tier, and for the pre-existing message-based markers).
 */
public class PossibleClientChannelCloseException extends ClosedChannelException {
}
