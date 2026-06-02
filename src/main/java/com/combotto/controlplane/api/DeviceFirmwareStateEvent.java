package com.combotto.controlplane.api;

import java.time.OffsetDateTime;

public record DeviceFirmwareStateEvent(
    String deviceId,
    String firmwareVersion,
    String previousFirmwareVersion,
    String jobId,
    String otaStatus,
    OffsetDateTime reportedAt,
    String sourceKind) {
}
