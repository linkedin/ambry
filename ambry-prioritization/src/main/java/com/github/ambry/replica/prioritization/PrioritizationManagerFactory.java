package com.github.ambry.replica.prioritization;

/**
 * Interface for Factory class which returns the {@link PrioritizationManager} depending on the implementation
 */
public interface PrioritizationManagerFactory {
  /**
   * @return returns the {@link PrioritizationManager}
   */
  PrioritizationManager getPrioritizationManager();
}
