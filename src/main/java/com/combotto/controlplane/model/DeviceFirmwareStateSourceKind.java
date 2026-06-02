package com.combotto.controlplane.model;

import java.util.Locale;
import java.util.Optional;

public enum DeviceFirmwareStateSourceKind {
  DEVICE_REPORT("device_report"),
  OTA_CONTROLLER("ota_controller");

  private final String value;

  DeviceFirmwareStateSourceKind(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  public static Optional<DeviceFirmwareStateSourceKind> from(String value) {
    if (value == null || value.isBlank()) {
      return Optional.of(DEVICE_REPORT);
    }

    String normalized = value.trim().toUpperCase(Locale.ROOT);
    for (DeviceFirmwareStateSourceKind sourceKind : values()) {
      if (sourceKind.name().equals(normalized) || sourceKind.value.equalsIgnoreCase(value.trim())) {
        return Optional.of(sourceKind);
      }
    }

    return Optional.empty();
  }
}
