/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.server;

import com.github.ambry.rest.RestRequest;
import com.github.ambry.router.Callback;
import java.io.Closeable;
import io.netty.channel.ChannelHandlerContext;

/**
 * Responsible for performing any security validations on the HTTP2 connection terminating on server. Validations could
 * involve authentication, authorization, security checks and so on which the implementation can decide. This could also
 * involve setting headers while responding, based on the request.
 * Exceptions are returned via {@link Callback}s on any validation failure.
 */
public interface  ServerSecurityService extends Closeable {

    /**
     * Performs security validations (if any) before allowing the HTTP2 connection setup to be complete and invokes the
     * {@code callback} once done.
     * @param ctx the {@link ChannelHandlerContext} to process.
     * @param callback the callback to invoke once processing is finished.
     */
    void validateConnection(ChannelHandlerContext ctx, Callback<Void> callback);

    /**
     * Performs security validations (if any) on the individual stream {@link RestRequest} asynchronously and invokes the
     * {@link Callback} when the validation completes.
     * @param restRequest {@link RestRequest} upon which validations has to be performed
     * @param callback The {@link Callback} which will be invoked on the completion of the request. Cannot be null.
     */
    void validateRequest(RestRequest restRequest, Callback<Void> callback);

}
