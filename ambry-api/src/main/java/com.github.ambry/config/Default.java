package com.github.ambry.config;

import java.lang.annotation.Documented;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;

@Documented
@Target(ElementType.FIELD)
public @interface Default
{
    String value();
}