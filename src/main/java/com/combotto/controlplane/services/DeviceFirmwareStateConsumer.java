package com.combotto.controlplane.services;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.combotto.controlplane.api.DeviceFirmwareStateEvent;
import com.combotto.controlplane.model.DeviceFirmwareOtaStatus;
import com.combotto.controlplane.model.DeviceFirmwareStateEntity;
import com.combotto.controlplane.model.DeviceFirmwareStateSourceKind;
import com.combotto.controlplane.repositories.AssetRepository;
import com.combotto.controlplane.repositories.DeviceFirmwareStateRepository;

@Component
public class DeviceFirmwareStateConsumer {

  private static final Logger log = LoggerFactory.getLogger(DeviceFirmwareStateConsumer.class);

  private final DeviceFirmwareStateRepository deviceFirmwareStateRepository;
  private final AssetRepository assetRepository;
  private final Clock clock;

  @Autowired
  public DeviceFirmwareStateConsumer(
      DeviceFirmwareStateRepository deviceFirmwareStateRepository,
      AssetRepository assetRepository) {
    this(deviceFirmwareStateRepository, assetRepository, Clock.systemUTC());
  }

  DeviceFirmwareStateConsumer(
      DeviceFirmwareStateRepository deviceFirmwareStateRepository,
      AssetRepository assetRepository,
      Clock clock) {
    this.deviceFirmwareStateRepository = deviceFirmwareStateRepository;
    this.assetRepository = assetRepository;
    this.clock = clock;
  }

  @KafkaListener(
      topics = "#{'${audit.kafka.topics.device-firmware-state:iot.device_firmware_state.v1}'.split(',')}",
      groupId = "${spring.kafka.consumer.device-firmware-state-group-id:control-plane-device-firmware-state-v1}",
      containerFactory = "deviceFirmwareStateKafkaListenerContainerFactory")
  @Transactional
  public void handle(ConsumerRecord<String, DeviceFirmwareStateEvent> record) {
    DeviceFirmwareStateEvent event = record.value();
    if (event == null) {
      logSkippedTombstone(record);
      return;
    }

    Optional<CanonicalDeviceFirmwareState> canonicalEvent =
        canonicalDeviceFirmwareState(event, record);
    if (canonicalEvent.isEmpty()) {
      return;
    }

    CanonicalDeviceFirmwareState stateEvent = canonicalEvent.get();
    Optional<DeviceFirmwareStateEntity> deviceFirmwareState =
        deviceFirmwareStateFor(stateEvent.deviceId());

    if (deviceFirmwareState.isEmpty()) {
      logUnknownDevice(stateEvent, record);
      return;
    }

    DeviceFirmwareStateEntity entity = deviceFirmwareState.get();
    recordDeviceFirmwareState(entity, stateEvent, record);
    deviceFirmwareStateRepository.save(entity);

    log.info(
        "Consumed device firmware state event device_id={} firmware_version={} status={}",
        stateEvent.deviceId(),
        stateEvent.firmwareVersion(),
        stateEvent.otaStatus());
  }

  private Optional<CanonicalDeviceFirmwareState> canonicalDeviceFirmwareState(
      DeviceFirmwareStateEvent event,
      ConsumerRecord<String, DeviceFirmwareStateEvent> record) {
    if (isBlank(event.deviceId()) || isBlank(event.firmwareVersion()) || isBlank(event.otaStatus())) {
      logSkippedMissingFields(record);
      return Optional.empty();
    }

    Optional<DeviceFirmwareOtaStatus> otaStatus = DeviceFirmwareOtaStatus.from(event.otaStatus());
    if (otaStatus.isEmpty()) {
      logSkippedUnknownStatus(event, record);
      return Optional.empty();
    }

    Optional<DeviceFirmwareStateSourceKind> sourceKind =
        DeviceFirmwareStateSourceKind.from(event.sourceKind());
    if (sourceKind.isEmpty()) {
      logSkippedUnknownSourceKind(event, record);
      return Optional.empty();
    }

    OffsetDateTime receivedAtUtc = currentTimestamp();
    OffsetDateTime reportedAt = event.reportedAt() != null ? event.reportedAt() : receivedAtUtc;

    return Optional.of(new CanonicalDeviceFirmwareState(
        event.deviceId().trim(),
        event.firmwareVersion().trim(),
        trimToNull(event.previousFirmwareVersion()),
        trimToNull(event.jobId()),
        otaStatus.get().name(),
        reportedAt,
        sourceKind.get().value(),
        receivedAtUtc));
  }

  private Optional<DeviceFirmwareStateEntity> deviceFirmwareStateFor(String deviceId) {
    return deviceFirmwareStateRepository
        .findByDeviceId(deviceId)
        .or(() -> newDeviceFirmwareState(deviceId));
  }

  private Optional<DeviceFirmwareStateEntity> newDeviceFirmwareState(String deviceId) {
    return assetRepository.findFirstActiveByDeviceIdentifier(deviceId)
        .map(asset -> {
          DeviceFirmwareStateEntity entity = new DeviceFirmwareStateEntity();
          entity.setAsset(asset);
          entity.setAssetId(asset.getId());
          entity.setDeviceId(deviceId);
          entity.setHardwareModel(asset.getHardwareModel());
          entity.setSecureBootEnabled(false);
          return entity;
        });
  }

  private void recordDeviceFirmwareState(
      DeviceFirmwareStateEntity entity,
      CanonicalDeviceFirmwareState event,
      ConsumerRecord<String, DeviceFirmwareStateEvent> record) {
    entity.setDeviceId(event.deviceId());
    entity.setJobId(event.jobId());
    entity.setOtaStatus(event.otaStatus());
    entity.setFirmwareVersion(event.firmwareVersion());
    entity.setPreviousFirmwareVersion(event.previousFirmwareVersion());
    entity.setReportedAt(event.reportedAt());
    entity.setReceivedAtUtc(event.receivedAtUtc());
    entity.setSourceKind(event.sourceKind());
    entity.setSourceTopic(record.topic());
    entity.setSourcePartition(record.partition());
    entity.setSourceOffset(record.offset());
    entity.setUpdatedAt(event.receivedAtUtc());
  }

  private void logSkippedTombstone(ConsumerRecord<String, DeviceFirmwareStateEvent> record) {
    log.info(
        "Skipped null/tombstone firmware state record topic={} partition={} offset={} key={}",
        record.topic(),
        record.partition(),
        record.offset(),
        record.key());
  }

  private void logSkippedMissingFields(ConsumerRecord<String, DeviceFirmwareStateEvent> record) {
    log.warn(
        "Skipped device firmware state event with missing required fields topic={} partition={} offset={} key={}",
        record.topic(),
        record.partition(),
        record.offset(),
        record.key());
  }

  private void logSkippedUnknownStatus(
      DeviceFirmwareStateEvent event,
      ConsumerRecord<String, DeviceFirmwareStateEvent> record) {
    log.warn(
        "Skipped device firmware state event with unknown ota_status={} topic={} partition={} offset={} key={}",
        event.otaStatus(),
        record.topic(),
        record.partition(),
        record.offset(),
        record.key());
  }

  private void logSkippedUnknownSourceKind(
      DeviceFirmwareStateEvent event,
      ConsumerRecord<String, DeviceFirmwareStateEvent> record) {
    log.warn(
        "Skipped device firmware state event with unknown source_kind={} topic={} partition={} offset={} key={}",
        event.sourceKind(),
        record.topic(),
        record.partition(),
        record.offset(),
        record.key());
  }

  private void logUnknownDevice(
      CanonicalDeviceFirmwareState event,
      ConsumerRecord<String, DeviceFirmwareStateEvent> record) {
    log.warn(
        "Skipped device firmware state for unknown device_id={} topic={} partition={} offset={}",
        event.deviceId(),
        record.topic(),
        record.partition(),
        record.offset());
  }

  private OffsetDateTime currentTimestamp() {
    return OffsetDateTime
        .now(clock)
        .withOffsetSameInstant(ZoneOffset.UTC)
        .truncatedTo(ChronoUnit.MILLIS);
  }

  private String trimToNull(String value) {
    return isBlank(value) ? null : value.trim();
  }

  private boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private record CanonicalDeviceFirmwareState(
      String deviceId,
      String firmwareVersion,
      String previousFirmwareVersion,
      String jobId,
      String otaStatus,
      OffsetDateTime reportedAt,
      String sourceKind,
      OffsetDateTime receivedAtUtc) {
  }
}
