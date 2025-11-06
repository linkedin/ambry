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
     * @param arguments Agent arguments in format: include=pkg1,pkg2;exclude=pkg3,pkg4
     * @param inst Instrumentation instance
     */
    public static void premain(String arguments, Instrumentation inst) {
        AgentConfig config = AgentConfig.parse(arguments);
        
        System.out.println("[ByteBufFlowAgent] Starting with config: " + config);
        
        // Setup JMX MBean for monitoring
        setupJmxMonitoring();
        
        // Setup shutdown hook for final report
        setupShutdownHook();
        
        // Build the agent
        new AgentBuilder.Default()
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
            .type(config.getTypeMatcher())
            .transform(new ByteBufTransformer())
            .installOn(inst);
        
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
                    // Match methods that might handle ByteBufs
                    isPublic()
                    .or(isProtected())
                    .and(not(isConstructor()))
                    .and(not(isStatic()))
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
    
    private AgentConfig(Set<String> includePackages, Set<String> excludePackages) {
        this.includePackages = includePackages;
        this.excludePackages = excludePackages;
    }
    
    /**
     * Parse agent arguments
     * Format: include=com.example,com.myapp;exclude=com.example.legacy
     */
    public static AgentConfig parse(String arguments) {
        Set<String> include = new HashSet<>();
        Set<String> exclude = new HashSet<>();
        
        if (arguments != null && !arguments.isEmpty()) {
            String[] parts = arguments.split(";");
            for (String part : parts) {
                String[] kv = part.split("=");
                if (kv.length == 2) {
                    String key = kv[0].trim();
                    String value = kv[1].trim();
                    
                    if ("include".equals(key)) {
                        include.addAll(Arrays.asList(value.split(",")));
                    } else if ("exclude".equals(key)) {
                        exclude.addAll(Arrays.asList(value.split(",")));
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
        
        return new AgentConfig(include, exclude);
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
    
    @Override
    public String toString() {
        return "AgentConfig{include=" + includePackages + ", exclude=" + excludePackages + "}";
    }
}
