package com.github.ambry.tools.perf.serverperf;

import com.github.ambry.network.ResponseInfo;


interface ResponseInfoProcessor {
  abstract void process(ResponseInfo responseInfo) throws Exception;
}
