package com.example.bytebuf.tracker.agent;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.utility.JavaModule;

import java.lang.instrument.Instrumentation;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static net.bytebuddy.matcher.ElementMatchers.*;

/**
 * Java Agent for ByteBuf flow tracking.
 * Instruments methods to track ByteBuf movement through the application.
 */
public class ByteBufFlowAgent {
    
    /**
     * Premain method for Java agent
     *
     * @param arguments Agent arguments in format: include=pkg1,pkg2;exclude=pkg3,pkg4;trackConstructors=class1,class2
     * @param inst Instrumentation instance
     */
    public static void premain(String arguments, Instrumentation inst) {
        AgentConfig config = AgentConfig.parse(arguments);

        System.out.println("[ByteBufFlowAgent] Starting with config: " + config);

        // Setup JMX MBean for monitoring
        setupJmxMonitoring();

        // Setup shutdown hook for final report
        setupShutdownHook();

        // Build the agent with chained transformers
        AgentBuilder.Identified.Extendable agentBuilder = new AgentBuilder.Default()
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
            .with(new AgentBuilder.Listener.StreamWriting(System.out).withTransformationsOnly())
            .ignore(
                nameStartsWith("net.bytebuddy.")
                .or(nameStartsWith("java."))
                .or(nameStartsWith("sun."))
                .or(nameStartsWith("com.sun."))
                .or(nameStartsWith("jdk."))
                .or(nameStartsWith("com.example.bytebuf.tracker.")) // Don't instrument ourselves
            )
            // Transform regular methods (non-constructors)
            .type(config.getTypeMatcher())
            .transform(new ByteBufTransformer());

        // Add constructor tracking for specified classes
        if (!config.getConstructorTrackingClasses().isEmpty()) {
            System.out.println("[ByteBufFlowAgent] Constructor tracking enabled for: " +
                config.getConstructorTrackingClasses());
            agentBuilder = agentBuilder
                .type(config.getConstructorTrackingMatcher())
                .transform(new ConstructorTrackingTransformer());
        }

        agentBuilder.installOn(inst);

        System.out.println("[ByteBufFlowAgent] Instrumentation installed successfully");
    }
    
    /**
     * Transformer that applies advice to methods
     */
    static class ByteBufTransformer implements AgentBuilder.Transformer {
        @Override
        public DynamicType.Builder<?> transform(
                DynamicType.Builder<?> builder,
                TypeDescription typeDescription,
                ClassLoader classLoader,
                JavaModule module) {

            return builder
                .method(
                    // Match methods that might handle ByteBufs (including static methods)
                    isPublic()
                    .or(isProtected())
                    .and(not(isConstructor()))
                )
                .intercept(Advice.to(ByteBufTrackingAdvice.class));
        }
    }

    /**
     * Transformer that applies advice to constructors for specified classes
     */
    static class ConstructorTrackingTransformer implements AgentBuilder.Transformer {
        @Override
        public DynamicType.Builder<?> transform(
                DynamicType.Builder<?> builder,
                TypeDescription typeDescription,
                ClassLoader classLoader,
                JavaModule module) {

            return builder
                .constructor(
                    // Match public and protected constructors
                    isPublic().or(isProtected())
                )
                .intercept(Advice.to(ByteBufTrackingAdvice.class));
        }
    }
    
    /**
     * Setup JMX MBean for runtime monitoring
     */
    private static void setupJmxMonitoring() {
        try {
            javax.management.MBeanServer mbs = 
                java.lang.management.ManagementFactory.getPlatformMBeanServer();
            javax.management.ObjectName name = 
                new javax.management.ObjectName("com.example:type=ByteBufFlowTracker");
            mbs.registerMBean(new ByteBufFlowMBean(), name);
            System.out.println("[ByteBufFlowAgent] JMX MBean registered");
        } catch (Exception e) {
            System.err.println("[ByteBufFlowAgent] Failed to register JMX MBean: " + e);
        }
    }
    
    /**
     * Setup shutdown hook to output final report
     */
    private static void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n=== ByteBuf Flow Final Report ===");
            ByteBufFlowReporter reporter = new ByteBufFlowReporter();
            System.out.println(reporter.generateReport());
        }));
    }
}

/**
 * Configuration for the agent
 */
class AgentConfig {
    private final Set<String> includePackages;
    private final Set<String> excludePackages;
    private final Set<String> constructorTrackingClasses;

    private AgentConfig(Set<String> includePackages, Set<String> excludePackages,
                        Set<String> constructorTrackingClasses) {
        this.includePackages = includePackages;
        this.excludePackages = excludePackages;
        this.constructorTrackingClasses = constructorTrackingClasses;
    }

    /**
     * Parse agent arguments
     * Format: include=com.example,com.myapp;exclude=com.example.legacy;trackConstructors=com.example.Message,com.example.Request
     */
    public static AgentConfig parse(String arguments) {
        Set<String> include = new HashSet<>();
        Set<String> exclude = new HashSet<>();
        Set<String> trackConstructors = new HashSet<>();

        if (arguments != null && !arguments.isEmpty()) {
            String[] parts = arguments.split(";");
            for (String part : parts) {
                String[] kv = part.split("=", 2); // Use limit=2 to handle class names with = in them
                if (kv.length == 2) {
                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    if ("include".equals(key)) {
                        include.addAll(Arrays.asList(value.split(",")));
                    } else if ("exclude".equals(key)) {
                        exclude.addAll(Arrays.asList(value.split(",")));
                    } else if ("trackConstructors".equals(key)) {
                        // Parse class names, trimming whitespace
                        for (String className : value.split(",")) {
                            trackConstructors.add(className.trim());
                        }
                    }
                }
            }
        }

        // Default to common application packages if none specified
        if (include.isEmpty()) {
            include.add("com.");
            include.add("org.");
            include.add("net.");
        }

        return new AgentConfig(include, exclude, trackConstructors);
    }

    /**
     * Get type matcher based on configuration
     */
    public ElementMatcher<TypeDescription> getTypeMatcher() {
        ElementMatcher<TypeDescription> matcher = none();

        // Include packages
        for (String pkg : includePackages) {
            matcher = matcher.or(nameStartsWith(pkg));
        }

        // Exclude packages
        for (String pkg : excludePackages) {
            matcher = matcher.and(not(nameStartsWith(pkg)));
        }

        return matcher;
    }

    /**
     * Get matcher for classes that should have constructor tracking
     */
    public ElementMatcher<TypeDescription> getConstructorTrackingMatcher() {
        ElementMatcher<TypeDescription> matcher = none();

        for (String className : constructorTrackingClasses) {
            // Support both exact matches and pattern matches
            if (className.endsWith("*")) {
                // Wildcard pattern: com.example.* matches all classes in package
                String prefix = className.substring(0, className.length() - 1);
                matcher = matcher.or(nameStartsWith(prefix));
            } else {
                // Exact match
                matcher = matcher.or(named(className));
            }
        }

        return matcher;
    }

    /**
     * Get the set of classes configured for constructor tracking
     */
    public Set<String> getConstructorTrackingClasses() {
        return constructorTrackingClasses;
    }

    @Override
    public String toString() {
        return "AgentConfig{include=" + includePackages +
               ", exclude=" + excludePackages +
               ", trackConstructors=" + constructorTrackingClasses + "}";
    }
}
