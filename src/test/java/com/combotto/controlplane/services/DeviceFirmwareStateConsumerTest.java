package com.combotto.controlplane.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.combotto.controlplane.api.DeviceFirmwareStateEvent;
import com.combotto.controlplane.model.AssetEntity;
import com.combotto.controlplane.model.DeviceFirmwareStateEntity;
import com.combotto.controlplane.repositories.AssetRepository;
import com.combotto.controlplane.repositories.DeviceFirmwareStateRepository;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
@ExtendWith(OutputCaptureExtension.class)
class DeviceFirmwareStateConsumerTest {

  @Mock
  private DeviceFirmwareStateRepository firmwareStateRepository;

  @Mock
  private AssetRepository assetRepository;

  private DeviceFirmwareStateConsumer consumer;
  private ObjectMapper objectMapper;

  private final Clock clock = Clock.fixed(Instant.parse("2026-06-01T12:00:00Z"), ZoneOffset.UTC);

  @BeforeEach
  void setUp() {
    consumer = new DeviceFirmwareStateConsumer(firmwareStateRepository, assetRepository, clock);
    objectMapper = JsonMapper.builder().findAndAddModules().build();
  }

  @Test
  void handleCreatesFirmwareStateForKnownAsset() {
    AssetEntity asset = asset("device-001");
    when(firmwareStateRepository.findByDeviceId("device-001")).thenReturn(Optional.empty());
    when(assetRepository.findFirstActiveByDeviceIdentifier("device-001")).thenReturn(Optional.of(asset));

    consumer.handle(record(event(
        "device-001",
        "1.2.3",
        "1.2.2",
        "job-123",
        "SUCCEEDED",
        OffsetDateTime.parse("2026-06-01T10:15:30Z"))));

    ArgumentCaptor<DeviceFirmwareStateEntity> captor =
        ArgumentCaptor.forClass(DeviceFirmwareStateEntity.class);
    verify(firmwareStateRepository).save(captor.capture());

    DeviceFirmwareStateEntity saved = captor.getValue();
    assertTrue(saved.isNew());
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

    consumer.handle(record(event(
        "device-001",
        "1.2.4",
        "1.2.3",
        "job-124",
        "failed",
        OffsetDateTime.parse("2026-06-01T11:00:00Z"))));

    ArgumentCaptor<DeviceFirmwareStateEntity> captor =
        ArgumentCaptor.forClass(DeviceFirmwareStateEntity.class);
    verify(firmwareStateRepository).save(captor.capture());

    DeviceFirmwareStateEntity saved = captor.getValue();
    assertEquals(existing, saved);
    assertEquals("job-124", saved.getJobId());
    assertEquals("FAILED", saved.getOtaStatus());
    assertEquals("1.2.4", saved.getFirmwareVersion());
    assertEquals("device_report", saved.getSourceKind());
    assertEquals(true, saved.isSecureBootEnabled());
  }

  @Test
  void handleDefaultsReportedAtAndSourceKind() {
    AssetEntity asset = asset("device-001");
    when(firmwareStateRepository.findByDeviceId("device-001")).thenReturn(Optional.empty());
    when(assetRepository.findFirstActiveByDeviceIdentifier("device-001")).thenReturn(Optional.of(asset));

    consumer.handle(record(event(
        "device-001",
        "1.2.3",
        null,
        null,
        "CURRENT",
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
  void handleUsesGatewayReceivedAtWhenPayloadReportedAtIsMissing() {
    AssetEntity asset = asset("device-001");
    when(firmwareStateRepository.findByDeviceId("device-001")).thenReturn(Optional.empty());
    when(assetRepository.findFirstActiveByDeviceIdentifier("device-001")).thenReturn(Optional.of(asset));

    consumer.handle(record(new DeviceFirmwareStateEvent(
        "firmware_update.v1",
        "devices/device-001/firmware/update",
        "device-001",
        "pi5-gateway-01",
        null,
        null,
        OffsetDateTime.parse("2026-06-01T11:59:58Z"),
        new DeviceFirmwareStateEvent.Payload(
            "device-001",
            "1.2.3",
            null,
            null,
            "CURRENT",
            "startup",
            null))));

    ArgumentCaptor<DeviceFirmwareStateEntity> captor =
        ArgumentCaptor.forClass(DeviceFirmwareStateEntity.class);
    verify(firmwareStateRepository).save(captor.capture());

    DeviceFirmwareStateEntity saved = captor.getValue();
    assertEquals(OffsetDateTime.parse("2026-06-01T11:59:58Z"), saved.getReportedAt());
    assertEquals(OffsetDateTime.parse("2026-06-01T12:00:00Z"), saved.getReceivedAtUtc());
  }

  @Test
  void deviceFirmwareStateEventDeserializesGatewayFirmwareRecord() throws Exception {
    String payload = """
        {
          "schema": "firmware_update.v1",
          "source_topic": "devices/device03/firmware/update",
          "device_id": "device03",
          "gateway_id": "pi5-gateway-01",
          "seq": null,
          "trace_id": "85a4b4406eda40e1c7133cceab7a806a",
          "received_at": "2026-06-01T11:59:58Z",
          "payload": {
            "device_id": "device03",
            "firmware_version": "1.5.2",
            "job_id": null,
            "ota_status": "UNKNOWN",
            "previous_firmware_version": null,
            "update_source": "startup"
          }
        }
        """;

    DeviceFirmwareStateEvent event =
        objectMapper.readValue(payload, DeviceFirmwareStateEvent.class);

    assertEquals("firmware_update.v1", event.schema());
    assertEquals("device03", event.deviceId());
    assertEquals("devices/device03/firmware/update", event.sourceTopic());
    assertEquals("pi5-gateway-01", event.gatewayId());
    assertEquals(OffsetDateTime.parse("2026-06-01T11:59:58Z"), event.receivedAt());
    assertEquals("1.5.2", event.payload().firmwareVersion());
    assertEquals("UNKNOWN", event.payload().otaStatus());
    assertEquals(null, event.payload().jobId());
    assertEquals(null, event.payload().previousFirmwareVersion());
  }

  @Test
  void kafkaDeserializerDeserializesGatewayFirmwareRecord() {
    String payload = """
        {
          "schema": "firmware_update.v1",
          "source_topic": "devices/device03/firmware/update",
          "device_id": "device03",
          "gateway_id": "pi5-gateway-01",
          "seq": null,
          "trace_id": "85a4b4406eda40e1c7133cceab7a806a",
          "received_at": "2026-06-01T11:59:58Z",
          "payload": {
            "device_id": "device03",
            "firmware_version": "1.5.2",
            "job_id": null,
            "ota_status": "UNKNOWN",
            "previous_firmware_version": null,
            "update_source": "startup"
          }
        }
        """;
    JacksonJsonDeserializer<DeviceFirmwareStateEvent> deserializer =
        new JacksonJsonDeserializer<>(DeviceFirmwareStateEvent.class);

    DeviceFirmwareStateEvent event = deserializer.deserialize(
        "iot.device_firmware_state.v1",
        new RecordHeaders(),
        payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));

    assertEquals("firmware_update.v1", event.schema());
    assertEquals("device03", event.deviceId());
    assertEquals("1.5.2", event.payload().firmwareVersion());
    assertEquals("UNKNOWN", event.payload().otaStatus());
    assertEquals(null, event.payload().jobId());
    assertEquals(null, event.payload().previousFirmwareVersion());
  }

  @Test
  void handleNormalizesUnknownFirmwareStatusToCurrent(CapturedOutput output) {
    AssetEntity asset = asset("device-001");
    when(firmwareStateRepository.findByDeviceId("device-001")).thenReturn(Optional.empty());
    when(assetRepository.findFirstActiveByDeviceIdentifier("device-001")).thenReturn(Optional.of(asset));

    consumer.handle(record(event(
        "device-001",
        "1.5.2",
        null,
        null,
        "UNKNOWN",
        null)));

    ArgumentCaptor<DeviceFirmwareStateEntity> captor =
        ArgumentCaptor.forClass(DeviceFirmwareStateEntity.class);
    verify(firmwareStateRepository).save(captor.capture());

    DeviceFirmwareStateEntity saved = captor.getValue();
    assertEquals("CURRENT", saved.getOtaStatus());
    assertEquals("1.5.2", saved.getFirmwareVersion());
    assertTrue(output.getOut().contains("ota_status=UNKNOWN to ota_status=CURRENT"));
  }

  @Test
  void handleNormalizesUnknownFirmwareStatusFromEnvelopeToCurrent(CapturedOutput output) throws Exception {
    AssetEntity asset = asset("device03");
    when(firmwareStateRepository.findByDeviceId("device03")).thenReturn(Optional.empty());
    when(assetRepository.findFirstActiveByDeviceIdentifier("device03")).thenReturn(Optional.of(asset));
    String payload = """
        {
          "schema": "firmware_update.v1",
          "source_topic": "devices/device03/firmware/update",
          "device_id": "device03",
          "gateway_id": "pi5-gateway-01",
          "seq": null,
          "trace_id": "85a4b4406eda40e1c7133cceab7a806a",
          "payload": {
            "device_id": "device03",
            "firmware_version": "1.5.2",
            "job_id": null,
            "ota_status": "UNKNOWN",
            "previous_firmware_version": null,
            "update_source": "startup"
          }
        }
        """;
    consumer.handle(record(objectMapper.readValue(payload, DeviceFirmwareStateEvent.class)));

    ArgumentCaptor<DeviceFirmwareStateEntity> captor =
        ArgumentCaptor.forClass(DeviceFirmwareStateEntity.class);
    verify(firmwareStateRepository).save(captor.capture());

    DeviceFirmwareStateEntity saved = captor.getValue();
    assertEquals("device03", saved.getDeviceId());
    assertEquals("CURRENT", saved.getOtaStatus());
    assertEquals("1.5.2", saved.getFirmwareVersion());
    assertTrue(output.getOut().contains("ota_status=UNKNOWN to ota_status=CURRENT"));
  }

  @Test
  void handleSkipsNullTombstoneValue() {
    consumer.handle(record(null));

    verify(firmwareStateRepository, never()).save(any());
  }

  @Test
  void handleSkipsMissingRequiredFields(CapturedOutput output) {
    consumer.handle(record(event(
        "device-001",
        null,
        null,
        null,
        "CURRENT",
        null)));

    verify(firmwareStateRepository, never()).save(any());
    assertTrue(output.getOut().contains("missing_fields=[firmware_version]"));
    assertTrue(output.getOut().contains("value_device_id=device-001"));
    assertTrue(output.getOut().contains("value_ota_status=CURRENT"));
  }

  @Test
  void handleSkipsInvalidStatus() {
    consumer.handle(record(event(
        "device-001",
        "1.2.3",
        null,
        null,
        "NOT_A_REAL_STATUS",
        null)));

    verify(firmwareStateRepository, never()).save(any());
  }

  @Test
  void handleSkipsUnknownSchema() {
    consumer.handle(record(new DeviceFirmwareStateEvent(
        "not_firmware_update.v1",
        "devices/device-001/firmware/update",
        "device-001",
        "pi5-gateway-01",
        null,
        null,
        null,
        new DeviceFirmwareStateEvent.Payload(
            "device-001",
            "1.2.3",
            null,
            null,
            "CURRENT",
            "startup",
            null))));

    verify(firmwareStateRepository, never()).save(any());
  }

  @Test
  void handleSkipsUnknownDevice() {
    when(firmwareStateRepository.findByDeviceId("missing-device")).thenReturn(Optional.empty());
    when(assetRepository.findFirstActiveByDeviceIdentifier("missing-device")).thenReturn(Optional.empty());

    consumer.handle(record(event(
        "missing-device",
        "1.2.3",
        null,
        null,
        "SUCCEEDED",
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

  private DeviceFirmwareStateEvent event(
      String deviceId,
      String firmwareVersion,
      String previousFirmwareVersion,
      String jobId,
      String otaStatus,
      OffsetDateTime reportedAt) {
    return new DeviceFirmwareStateEvent(
        "firmware_update.v1",
        "devices/" + deviceId + "/firmware/update",
        deviceId,
        "pi5-gateway-01",
        null,
        null,
        null,
        new DeviceFirmwareStateEvent.Payload(
            deviceId,
            firmwareVersion,
            previousFirmwareVersion,
            jobId,
            otaStatus,
            "startup",
            reportedAt));
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
