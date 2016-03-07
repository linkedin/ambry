package com.github.ambry.rest;

import java.util.HashSet;
import java.util.Set;
import javax.servlet.http.Cookie;


/**
 * Common utility functions that will be used in Netty layer
 */
public class NettyUtils {

  /**
   * Converts the Set of {@link javax.servlet.http.Cookie}s to equivalent {@link javax.servlet.http.Cookie}s
   * @param httpCookies Set of {@link javax.servlet.http.Cookie}s that needs to be converted
   * @return Set of {@link javax.servlet.http.Cookie}s equivalent to the passed in {@link javax.servlet.http.Cookie}s
   */
  public static Set<Cookie> convertHttpToJavaCookies(Set<io.netty.handler.codec.http.Cookie> httpCookies) {
    Set<javax.servlet.http.Cookie> cookies = new HashSet<Cookie>();
    for (io.netty.handler.codec.http.Cookie cookie : httpCookies) {
      javax.servlet.http.Cookie javaCookie = new javax.servlet.http.Cookie(cookie.getName(), cookie.getValue());
      javaCookie.setSecure(cookie.isSecure());
      javaCookie.setMaxAge((int) cookie.getMaxAge());
      javaCookie.setHttpOnly(cookie.isHttpOnly());
      javaCookie.setVersion(cookie.getVersion());
      if (cookie.getPath() != null) {
        javaCookie.setPath(cookie.getPath());
      }
      if (cookie.getComment() != null) {
        javaCookie.setComment(cookie.getComment());
      }
      if (cookie.getDomain() != null) {
        javaCookie.setDomain(cookie.getDomain());
      }
      cookies.add(javaCookie);
    }
    return cookies;
  }
}
