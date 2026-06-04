package com.combotto.controlplane.api;

import java.time.OffsetDateTime;

import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record DeviceFirmwareStateEvent(
    String schema,
    String sourceTopic,
    String deviceId,
    String gatewayId,
    Long seq,
    String traceId,
    OffsetDateTime receivedAt,
    Payload payload) {

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public record Payload(
      String deviceId,
      String firmwareVersion,
      String previousFirmwareVersion,
      String jobId,
      String otaStatus,
      String updateSource,
      OffsetDateTime reportedAt) {
  }
}
