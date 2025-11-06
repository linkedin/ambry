package com.example.bytebuf.tracker.agent;

import com.example.bytebuf.tracker.ByteBufFlowTracker;
import com.example.bytebuf.tracker.ObjectTrackerHandler;
import com.example.bytebuf.tracker.ObjectTrackerRegistry;
import net.bytebuddy.asm.Advice;

/**
 * ByteBuddy advice for tracking object flow through methods.
 * Originally designed for ByteBuf, but now supports any object via ObjectTrackerHandler.
 */
public class ByteBufTrackingAdvice {

    /**
     * Method entry advice - tracks objects in parameters
     */
    @Advice.OnMethodEnter
    public static void onMethodEnter(
            @Advice.Origin Class<?> clazz,
            @Advice.Origin("#m") String methodName,
            @Advice.AllArguments Object[] arguments) {

        if (arguments == null || arguments.length == 0) {
            return;
        }

        ObjectTrackerHandler handler = ObjectTrackerRegistry.getHandler();
        ByteBufFlowTracker tracker = ByteBufFlowTracker.getInstance();

        for (Object arg : arguments) {
            if (handler.shouldTrack(arg)) {
                int metric = handler.getMetric(arg);
                tracker.recordMethodCall(
                    arg,
                    clazz.getSimpleName(),
                    methodName,
                    metric
                );
            }
        }
    }

    /**
     * Method exit advice - tracks objects in return values and final state
     */
    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void onMethodExit(
            @Advice.Origin Class<?> clazz,
            @Advice.Origin("#m") String methodName,
            @Advice.AllArguments Object[] arguments,
            @Advice.Return Object returnValue,
            @Advice.Thrown Throwable thrown) {

        ObjectTrackerHandler handler = ObjectTrackerRegistry.getHandler();
        ByteBufFlowTracker tracker = ByteBufFlowTracker.getInstance();

        // Check if any tracked objects in parameters have changed state
        if (arguments != null) {
            for (Object arg : arguments) {
                if (handler.shouldTrack(arg)) {
                    int metric = handler.getMetric(arg);
                    // Record the exit state
                    tracker.recordMethodCall(
                        arg,
                        clazz.getSimpleName(),
                        methodName + "_exit",
                        metric
                    );
                }
            }
        }

        // Track return values
        if (handler.shouldTrack(returnValue)) {
            int metric = handler.getMetric(returnValue);
            tracker.recordMethodCall(
                returnValue,
                clazz.getSimpleName(),
                methodName + "_return",
                metric
            );
        }
    }
}
