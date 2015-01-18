package com.github.ambry.router;

/**
 * A callback interface that the user can implement to allow code to execute when the request is complete. This callback
 * will generally execute in the background I/O thread so it should be fast.
 */
public interface Callback<T> {
  /**
   * The callback method that the user can implement to do asynchronous handling of the response. T defines the type of
   * the result. When T=Void then there is no specific response expected but any errors are reported on the
   * exception object. For all other values of T, one of the two arguments would be null.
   * @param result The result of the request. This would be non null when the request executed successfully
   * @param exception The exception that was reported on execution of the request
   */
  public void onCompletion(T result, Exception exception);
}
