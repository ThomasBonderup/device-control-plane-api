package com.combotto.controlplane.model;

import java.util.Locale;
import java.util.Optional;

public enum DeviceFirmwareOtaStatus {
  CURRENT,
  REQUESTED,
  QUEUED,
  DOWNLOADING,
  INSTALLING,
  IN_PROGRESS,
  SUCCEEDED,
  FAILED,
  REJECTED,
  CANCELED,
  TIMED_OUT;

  public static Optional<DeviceFirmwareOtaStatus> from(String value) {
    if (value == null || value.isBlank()) {
      return Optional.empty();
    }

    try {
      return Optional.of(valueOf(value.trim().toUpperCase(Locale.ROOT)));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
