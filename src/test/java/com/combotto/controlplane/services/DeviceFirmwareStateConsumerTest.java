package com.combotto.controlplane.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.combotto.controlplane.api.DeviceFirmwareStateEvent;
import com.combotto.controlplane.model.AssetEntity;
import com.combotto.controlplane.model.DeviceFirmwareStateEntity;
import com.combotto.controlplane.repositories.AssetRepository;
import com.combotto.controlplane.repositories.DeviceFirmwareStateRepository;

@ExtendWith(MockitoExtension.class)
class DeviceFirmwareStateConsumerTest {

  @Mock
  private DeviceFirmwareStateRepository firmwareStateRepository;

  @Mock
  private AssetRepository assetRepository;

  private DeviceFirmwareStateConsumer consumer;

  private final Clock clock = Clock.fixed(Instant.parse("2026-06-01T12:00:00Z"), ZoneOffset.UTC);

  @BeforeEach
  void setUp() {
    consumer = new DeviceFirmwareStateConsumer(firmwareStateRepository, assetRepository, clock);
  }

  @Test
  void handleCreatesFirmwareStateForKnownAsset() {
    AssetEntity asset = asset("device-001");
    when(firmwareStateRepository.findByDeviceId("device-001")).thenReturn(Optional.empty());
    when(assetRepository.findFirstActiveByDeviceIdentifier("device-001")).thenReturn(Optional.of(asset));

    consumer.handle(record(new DeviceFirmwareStateEvent(
        "device-001",
        "1.2.3",
        "1.2.2",
        "job-123",
        "SUCCEEDED",
        OffsetDateTime.parse("2026-06-01T10:15:30Z"),
        null)));

    ArgumentCaptor<DeviceFirmwareStateEntity> captor =
        ArgumentCaptor.forClass(DeviceFirmwareStateEntity.class);
    verify(firmwareStateRepository).save(captor.capture());

    DeviceFirmwareStateEntity saved = captor.getValue();
    assertEquals(99L, saved.getAssetId());
    assertEquals(asset, saved.getAsset());
    assertEquals("device-001", saved.getDeviceId());
    assertEquals("job-123", saved.getJobId());
    assertEquals("SUCCEEDED", saved.getOtaStatus());
    assertEquals("1.2.3", saved.getFirmwareVersion());
    assertEquals("1.2.2", saved.getPreviousFirmwareVersion());
    assertEquals(OffsetDateTime.parse("2026-06-01T10:15:30Z"), saved.getReportedAt());
    assertEquals(OffsetDateTime.parse("2026-06-01T12:00:00Z"), saved.getReceivedAtUtc());
    assertEquals("device_report", saved.getSourceKind());
    assertEquals("iot.device_firmware_state.v1", saved.getSourceTopic());
    assertEquals(2, saved.getSourcePartition());
    assertEquals(42L, saved.getSourceOffset());
    assertEquals(OffsetDateTime.parse("2026-06-01T12:00:00Z"), saved.getUpdatedAt());
    assertFalse(saved.isSecureBootEnabled());
  }

  @Test
  void handleUpdatesExistingFirmwareStateAndPreservesSecureBoot() {
    DeviceFirmwareStateEntity existing = new DeviceFirmwareStateEntity();
    existing.setAssetId(99L);
    existing.setDeviceId("device-001");
    existing.setSecureBootEnabled(true);
    when(firmwareStateRepository.findByDeviceId("device-001")).thenReturn(Optional.of(existing));

    consumer.handle(record(new DeviceFirmwareStateEvent(
        "device-001",
        "1.2.4",
        "1.2.3",
        "job-124",
        "failed",
        OffsetDateTime.parse("2026-06-01T11:00:00Z"),
        "ota_controller")));

    ArgumentCaptor<DeviceFirmwareStateEntity> captor =
        ArgumentCaptor.forClass(DeviceFirmwareStateEntity.class);
    verify(firmwareStateRepository).save(captor.capture());

    DeviceFirmwareStateEntity saved = captor.getValue();
    assertEquals(existing, saved);
    assertEquals("job-124", saved.getJobId());
    assertEquals("FAILED", saved.getOtaStatus());
    assertEquals("1.2.4", saved.getFirmwareVersion());
    assertEquals("ota_controller", saved.getSourceKind());
    assertEquals(true, saved.isSecureBootEnabled());
  }

  @Test
  void handleDefaultsReportedAtAndSourceKind() {
    AssetEntity asset = asset("device-001");
    when(firmwareStateRepository.findByDeviceId("device-001")).thenReturn(Optional.empty());
    when(assetRepository.findFirstActiveByDeviceIdentifier("device-001")).thenReturn(Optional.of(asset));

    consumer.handle(record(new DeviceFirmwareStateEvent(
        "device-001",
        "1.2.3",
        null,
        null,
        "CURRENT",
        null,
        null)));

    ArgumentCaptor<DeviceFirmwareStateEntity> captor =
        ArgumentCaptor.forClass(DeviceFirmwareStateEntity.class);
    verify(firmwareStateRepository).save(captor.capture());

    DeviceFirmwareStateEntity saved = captor.getValue();
    assertEquals(OffsetDateTime.parse("2026-06-01T12:00:00Z"), saved.getReportedAt());
    assertEquals(OffsetDateTime.parse("2026-06-01T12:00:00Z"), saved.getReceivedAtUtc());
    assertEquals("device_report", saved.getSourceKind());
  }

  @Test
  void handleSkipsNullTombstoneValue() {
    consumer.handle(record(null));

    verify(firmwareStateRepository, never()).save(any());
  }

  @Test
  void handleSkipsMissingRequiredFields() {
    consumer.handle(record(new DeviceFirmwareStateEvent(
        "device-001",
        null,
        null,
        null,
        "CURRENT",
        null,
        null)));

    verify(firmwareStateRepository, never()).save(any());
  }

  @Test
  void handleSkipsInvalidStatus() {
    consumer.handle(record(new DeviceFirmwareStateEvent(
        "device-001",
        "1.2.3",
        null,
        null,
        "NOT_A_REAL_STATUS",
        null,
        null)));

    verify(firmwareStateRepository, never()).save(any());
  }

  @Test
  void handleSkipsInvalidSourceKind() {
    consumer.handle(record(new DeviceFirmwareStateEvent(
        "device-001",
        "1.2.3",
        null,
        null,
        "CURRENT",
        null,
        "not_a_source")));

    verify(firmwareStateRepository, never()).save(any());
  }

  @Test
  void handleSkipsUnknownDevice() {
    when(firmwareStateRepository.findByDeviceId("missing-device")).thenReturn(Optional.empty());
    when(assetRepository.findFirstActiveByDeviceIdentifier("missing-device")).thenReturn(Optional.empty());

    consumer.handle(record(new DeviceFirmwareStateEvent(
        "missing-device",
        "1.2.3",
        null,
        null,
        "SUCCEEDED",
        null,
        null)));

    verify(firmwareStateRepository, never()).save(any());
  }

  private ConsumerRecord<String, DeviceFirmwareStateEvent> record(DeviceFirmwareStateEvent event) {
    return new ConsumerRecord<>(
        "iot.device_firmware_state.v1",
        2,
        42L,
        "device-001",
        event);
  }

  private AssetEntity asset(String deviceId) {
    AssetEntity asset = new AssetEntity();
    asset.setId(99L);
    asset.setSerialNumber(deviceId);
    asset.setExternalRef(deviceId);
    asset.setHardwareModel("stm32");
    return asset;
  }
}
