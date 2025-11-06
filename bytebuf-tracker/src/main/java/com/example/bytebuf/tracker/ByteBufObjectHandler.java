package com.example.bytebuf.tracker;

import io.netty.buffer.ByteBuf;

/**
 * Default handler for tracking ByteBuf objects.
 *
 * This is the default implementation used when no custom handler is specified.
 * It tracks Netty ByteBuf objects and uses their reference count as the metric.
 */
public class ByteBufObjectHandler implements ObjectTrackerHandler {

    @Override
    public boolean shouldTrack(Object obj) {
        return obj instanceof ByteBuf;
    }

    @Override
    public int getMetric(Object obj) {
        if (obj instanceof ByteBuf) {
            return ((ByteBuf) obj).refCnt();
        }
        return 0;
    }

    @Override
    public String getObjectType() {
        return "ByteBuf";
    }
}
