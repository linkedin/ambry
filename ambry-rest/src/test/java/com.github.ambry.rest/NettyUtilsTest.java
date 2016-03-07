package com.github.ambry.rest;

import io.netty.handler.codec.http.Cookie;
import io.netty.handler.codec.http.DefaultCookie;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import junit.framework.Assert;
import org.junit.Test;


public class NettyUtilsTest {

  @Test
  public void convertHttpToJavaCookiesTest() {
    Cookie cookie = new DefaultCookie("CookieKey1", "CookieValue1");
    cookie.setVersion(1);
    cookie.setHttpOnly(true);
    cookie.setDomain("domain1");
    cookie.setComment("comment1");
    cookie.setCommentUrl("commentUrl1");
    cookie.setPath("path1Dir1/path1File1");
    int maxAge = new Random().nextInt(10000);
    cookie.setMaxAge(maxAge);
    cookie.setDiscard(false);
    Set<Cookie> cookies = new HashSet<Cookie>();
    cookies.add(cookie);
    Set<javax.servlet.http.Cookie> javaCookies = NettyUtils.convertHttpToJavaCookies(cookies);
    Assert.assertEquals("Size mistmatch ", cookies.size(), javaCookies.size());
    compareCookies(cookies, javaCookies);

    cookie = new DefaultCookie("CookieKey2", "CookieValue2");
    cookie.setVersion(1);
    cookie.setHttpOnly(false);
    cookie.setDomain("domain2");
    cookie.setComment("comment2");
    cookie.setCommentUrl("commentUrl2");
    cookie.setPath("path1Dir2/path1File2");
    maxAge = new Random().nextInt(10000);
    cookie.setMaxAge(maxAge);
    cookie.setDiscard(false);
    cookies.add(cookie);
    javaCookies = NettyUtils.convertHttpToJavaCookies(cookies);
    Assert.assertEquals("Size mistmatch ", cookies.size(), javaCookies.size());
    compareCookies(cookies, javaCookies);
  }

  /**
   * Compares a set of {@link Cookie} with that of {@link javax.servlet.http.Cookie}s for equality
   * @param httpCookies Set of {@link Cookie}s to be compared with the {@code javaCookies}
   * @param javaCookies Set of {@link javax.servlet.http.Cookie}s to be compared with those of {@code httpCookies}
   */
  static void compareCookies(Set<Cookie> httpCookies, Set<javax.servlet.http.Cookie> javaCookies) {
    if (httpCookies.size() != javaCookies.size()) {
      org.junit.Assert.fail("Size of cookies didn't match");
    } else {
      HashMap<String, Cookie> cookieHashMap = new HashMap<String, Cookie>();
      for (Cookie cookie : httpCookies) {
        cookieHashMap.put(cookie.getName(), cookie);
      }
      HashMap<String, javax.servlet.http.Cookie> javaCookiesHashMap = new HashMap<String, javax.servlet.http.Cookie>();
      for (javax.servlet.http.Cookie cookie : javaCookies) {
        javaCookiesHashMap.put(cookie.getName(), cookie);
      }
      for (String cookieName : cookieHashMap.keySet()) {
        Cookie cookie = cookieHashMap.get(cookieName);
        compareCookie(cookie, javaCookiesHashMap.get(cookieName));
        javaCookiesHashMap.remove(cookieName);
      }
      org.junit.Assert.assertEquals("More Cookies found in NettyRequest ", 0, javaCookiesHashMap.size());
    }
  }

  /**
   * Compare {@link Cookie} with {@link javax.servlet.http.Cookie}
   * @param httpCookie {@link javax.servlet.http.Cookie} to be compared with {@code javaCookie}
   * @param javaCookie {@link javax.servlet.http.Cookie} to be compared with {@code httpCookie}
   */
  static void compareCookie(Cookie httpCookie, javax.servlet.http.Cookie javaCookie) {
    org.junit.Assert.assertEquals("Value field didn't match ", httpCookie.getValue(), javaCookie.getValue());
    org.junit.Assert.assertEquals("Secure field didn't match ", httpCookie.isSecure(), javaCookie.getSecure());
    org.junit.Assert.assertEquals("Max Age field didn't match ", httpCookie.getMaxAge(), javaCookie.getMaxAge());
    org.junit.Assert.assertEquals("Max Age field didn't match ", httpCookie.isHttpOnly(), javaCookie.isHttpOnly());
    org.junit.Assert.assertEquals("Max Age field didn't match ", httpCookie.getVersion(), javaCookie.getVersion());
    if (httpCookie.getPath() != null) {
      org.junit.Assert.assertEquals("Path field didn't match ", httpCookie.getPath(), javaCookie.getPath());
    } else {
      org.junit.Assert.assertTrue("Path field didn't match", (javaCookie.getPath() == null));
    }
    if (httpCookie.getComment() != null) {
      org.junit.Assert.assertEquals("Comment field didn't match ", httpCookie.getComment(), javaCookie.getComment());
    } else {
      org.junit.Assert.assertTrue("Comment field didn't match ", (javaCookie.getComment() == null));
    }

    if (httpCookie.getDomain() != null) {
      org.junit.Assert.assertEquals("Domain field didn't match ", httpCookie.getDomain(), javaCookie.getDomain());
    } else {
      org.junit.Assert.assertTrue("Domain field  didn't match ", (javaCookie.getDomain() == null));
    }
  }
}
