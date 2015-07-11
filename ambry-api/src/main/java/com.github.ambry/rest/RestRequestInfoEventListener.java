package com.github.ambry.rest;

/**
 * This event listener provides a way to be informed of handling completion of a particular {@link RestRequestInfo}.
 * Since {@link RestRequestInfo} objects might be processed async, any operations that need to be performed once
 * handling is completed need to be done through this callback.
 */
public interface RestRequestInfoEventListener {

  /**
   * Called when the handling of a particular {@link RestRequestInfo} is complete.
   * <p/>
   * If e is non-null, then the handling has failed and e contains the {@link Exception} that caused handling to fail.
   * @param restRequestInfo - the {@link RestRequestInfo} whose handling is now complete.
   * @param e - any {@link Exception} that was encountered while handling this {@link RestRequestInfo}. If non-null,
   *          handling failed.
   */
  public void onCompleted(RestRequestInfo restRequestInfo, Exception e);
}
