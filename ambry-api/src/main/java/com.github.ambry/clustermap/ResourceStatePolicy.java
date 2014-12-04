package com.github.ambry.clustermap;

/** ResourceStatePolicy is used to determine if the state of a resource is "up" or "down". For resources like data nodes
 *  and disks, up and down mean available and unavailable, respectively.
 */
public interface ResourceStatePolicy {
  /**
   * Checks to see if the state is permanently down.
   *
   * @return true if the state is permanently down, false otherwise.
   */
  public boolean isHardDown();

  /**
   * Checks to see if the state is down (soft or hard).
   *
   * @return true if the state is down, false otherwise.
   */
  public boolean isDown();

  /**
   * Should be called by the caller every time an error is encountered for the corresponding resource.
   */
  public void onError();
}

