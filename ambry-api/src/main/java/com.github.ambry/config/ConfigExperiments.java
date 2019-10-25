package com.github.ambry.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.function.Function;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.matcher.ElementMatchers;


public class ConfigExperiments {
  // annotation / bytecode generation based

  public interface Config {
    // Annotations
    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Key {
      String value();
    }

    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface NumericDefault {
      long value();
    }

    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Min {
      long value();
    }

    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Max {
      long value();
    }

    @Documented
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @interface StringLikeDefault {
      String value();
    }

  }

  public static class ConfigUtils {
    @SuppressWarnings("unchecked")
    static <T extends Config> T populate(Class<T> configType, VerifiableProperties props) {
      if (!configType.isInterface()) {
        throw new IllegalArgumentException(configType + " is not an interface");
      }

      DynamicType.Builder<T> builder = new ByteBuddy().subclass(configType);
      for (Method method : configType.getMethods()) {
        Config.Key key = method.getAnnotation(Config.Key.class);
        if (key != null) {
          Class<?> type = method.getReturnType();
          if (Integer.TYPE.equals(type) || Integer.class.equals(type)) {
            int value;
            Config.NumericDefault numericDefault = method.getAnnotation(Config.NumericDefault.class);
            if (numericDefault != null) {
              int defaultValue = ConfigUtils.checkCastInt(numericDefault.value());
              Config.Min min = method.getAnnotation(Config.Min.class);
              Config.Max max = method.getAnnotation(Config.Max.class);
              if (min != null || max != null) {
                int minValue = min != null ? ConfigUtils.checkCastInt(min.value()) : Integer.MIN_VALUE;
                int maxValue = min != null ? ConfigUtils.checkCastInt(max.value()) : Integer.MAX_VALUE;
                value = props.getIntInRange(key.value(), defaultValue, minValue, maxValue);
              } else {
                value = props.getInt(key.value(), defaultValue);
              }
            } else {
              Config.Min min = method.getAnnotation(Config.Min.class);
              Config.Max max = method.getAnnotation(Config.Max.class);
              if (min != null || max != null) {
                int minValue = min != null ? ConfigUtils.checkCastInt(min.value()) : Integer.MIN_VALUE;
                int maxValue = min != null ? ConfigUtils.checkCastInt(max.value()) : Integer.MAX_VALUE;
                value = props.getIntInRange(key.value(), minValue, maxValue);
              } else {
                value = props.getInt(key.value());
              }
            }
            builder = builder.method(ElementMatchers.is(method)).intercept(FixedValue.value(value));
          } else {
            String stringValue;
            Config.StringLikeDefault stringLikeDefault = method.getAnnotation(Config.StringLikeDefault.class);
            if (stringLikeDefault != null) {
              String defaultValue = stringLikeDefault.value();
              stringValue = props.getString(key.value(), defaultValue);
            } else {
              stringValue = props.getString(key.value());
            }
            Object value;
            if (type.isEnum()) {
              value = Enum.valueOf((Class<? extends Enum>) type, stringValue);
            } else if (type.isAssignableFrom(String.class)) {
              value = stringValue;
            } else {
              throw new IllegalArgumentException("Unsupported config value type for " + method);
            }
            builder = builder.method(ElementMatchers.is(method)).intercept(FixedValue.value(value));
          }
        } else {
          throw new UnsupportedOperationException("Config type unsupported for " + method);
        }
      }
      try {
        return builder.make().load(configType.getClassLoader()).getLoaded().newInstance();
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("Reflection error while creating config", e);
      }
    }

    private static int checkCastInt(long num) {
      if (num != (int) num) {
        throw new IllegalArgumentException("Not a valid int: " + num);
      }
      return (int) num;
    }
  }

  enum Coolness {
    COOL, UNCOOL
  }

  public interface FooConfig1 extends Config {
    @Key("foo.wah")
    @NumericDefault(22)
    @Min(-2)
    @Max(55)
    int wah();

    @Key("foo.blah")
    @StringLikeDefault("abcdef")
    String blah();

    @Key("foo.cool")
    @StringLikeDefault("UNCOOL")
    Coolness cool();
  }

  // ConfigDef class based

  public static class ConfigDef<T> {
    private final String name;
    private final Function<VerifiableProperties, T> extractor;

    private ConfigDef(String name, Function<VerifiableProperties, T> extractor) {
      this.name = name;
      this.extractor = extractor;
    }

    public String getName() {
      return name;
    }

    T getValue(VerifiableProperties verifiableProperties) {
      return extractor.apply(verifiableProperties);
    }

    public static ConfigDef<Integer> forInt(String name) {
      return new ConfigDef<>(name, p -> p.getInt(name));
    }

    public static ConfigDef<Integer> forInt(String name, int defaultVal) {
      return new ConfigDef<>(name, p -> p.getInt(name, defaultVal));
    }

    public static ConfigDef<Integer> forIntInRange(String name, int defaultVal, int start, int end) {
      return new ConfigDef<>(name, p -> p.getIntInRange(name, defaultVal, start, end));
    }
    public static <E extends Enum<E>> ConfigDef<E> forEnum(String name, Class<E> enumType) {
      return new ConfigDef<>(name, p -> Enum.valueOf(enumType, p.getString(name)));
    }

    public static <E extends Enum<E>> ConfigDef<E> forEnum(String name, Class<E> enumType, E defaultVal) {
      return new ConfigDef<>(name, p -> Enum.valueOf(enumType, p.getString(name, defaultVal.name())));
    }

    public static ConfigDef<String> forString(String name) {
      return new ConfigDef<>(name, p -> p.getString(name));
    }

    public static ConfigDef<String> forString(String name, String defaultVal) {
      return new ConfigDef<>(name, p -> p.getString(name, defaultVal));
    }
  }

  public static class FooConfig2 {
    public static final ConfigDef<Integer> WAH = ConfigDef.forIntInRange("foo.wah", 22, -2, 55);
    public final int wah;

    public static final ConfigDef<String> BLAH = ConfigDef.forString("foo.wah", "abcdef");
    public final String blah;

    public static final ConfigDef<Coolness> COOL = ConfigDef.forEnum("foo.cool", Coolness.class, Coolness.UNCOOL);
    public final Coolness cool;

    public FooConfig2(VerifiableProperties verifiableProperties) {
      wah = WAH.getValue(verifiableProperties);
      blah = BLAH.getValue(verifiableProperties);
      cool = COOL.getValue(verifiableProperties);
    }
  }

  // demo

  public static void main(String[] args) {
    FooConfig1 conf1 = ConfigUtils.populate(FooConfig1.class, new VerifiableProperties(new Properties()));
    System.out.println("conf1.wah() = " + conf1.wah());
    System.out.println("conf1.blah() = " + conf1.blah());
    System.out.println("conf1.cool() = " + conf1.cool());

    FooConfig2 conf2 = new FooConfig2(new VerifiableProperties(new Properties()));
    System.out.println("conf2.wah = " + conf2.wah);
    System.out.println("conf2.blah = " + conf2.blah);
    System.out.println("conf2.cool = " + conf2.cool);
  }

}

