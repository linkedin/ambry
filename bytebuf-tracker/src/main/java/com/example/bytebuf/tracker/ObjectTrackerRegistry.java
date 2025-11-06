package com.example.bytebuf.tracker;

/**
 * Registry for the object tracker handler.
 *
 * This singleton holds the handler used by the ByteBuddy advice.
 * Users can set a custom handler to track objects other than ByteBuf.
 *
 * Usage:
 * 1. Create your custom ObjectTrackerHandler implementation
 * 2. Set it BEFORE the agent starts tracking (ideally in static initializer or main())
 * 3. Or specify via system property: -Dobject.tracker.handler=com.yourcompany.YourHandler
 */
public class ObjectTrackerRegistry {

    private static volatile ObjectTrackerHandler handler = null;
    private static final Object LOCK = new Object();

    /**
     * Get the current handler, creating default if none set.
     */
    public static ObjectTrackerHandler getHandler() {
        if (handler == null) {
            synchronized (LOCK) {
                if (handler == null) {
                    // Try to load from system property first
                    String handlerClassName = System.getProperty("object.tracker.handler");
                    if (handlerClassName != null && !handlerClassName.isEmpty()) {
                        try {
                            Class<?> handlerClass = Class.forName(handlerClassName);
                            handler = (ObjectTrackerHandler) handlerClass.getDeclaredConstructor().newInstance();
                            System.out.println("[ObjectTrackerRegistry] Loaded custom handler: " + handlerClassName);
                        } catch (Exception e) {
                            System.err.println("[ObjectTrackerRegistry] Failed to load handler: " + handlerClassName);
                            e.printStackTrace();
                            handler = new ByteBufObjectHandler(); // Fall back to default
                        }
                    } else {
                        // Use default ByteBuf handler
                        handler = new ByteBufObjectHandler();
                        System.out.println("[ObjectTrackerRegistry] Using default ByteBuf handler");
                    }
                }
            }
        }
        return handler;
    }

    /**
     * Set a custom handler for tracking objects.
     *
     * This should be called early in your application, ideally in a static
     * initializer or at the very beginning of main(), before any tracked
     * objects are created.
     *
     * @param customHandler Your custom handler implementation
     */
    public static void setHandler(ObjectTrackerHandler customHandler) {
        synchronized (LOCK) {
            handler = customHandler;
            System.out.println("[ObjectTrackerRegistry] Custom handler set: " +
                customHandler.getClass().getName() + " tracking " + customHandler.getObjectType());
        }
    }

    /**
     * Reset to default handler (mainly for testing).
     */
    public static void resetToDefault() {
        synchronized (LOCK) {
            handler = new ByteBufObjectHandler();
        }
    }
}
