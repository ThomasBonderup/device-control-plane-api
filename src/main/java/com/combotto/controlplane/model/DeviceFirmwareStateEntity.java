package com.combotto.controlplane.model;

import java.time.OffsetDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "device_firmware_state", schema = "control_plane")
public class DeviceFirmwareStateEntity {

  @Id
  @Column(name = "asset_id")
  private Long assetId;

  @OneToOne(fetch = FetchType.LAZY, optional = false)
  @MapsId
  @JoinColumn(name = "asset_id", nullable = false)
  private AssetEntity asset;

  @Column(name = "device_id", nullable = false)
  private String deviceId;

  @Column(name = "job_id")
  private String jobId;

  @Column(name = "ota_status", nullable = false)
  private String otaStatus;

  @Column(name = "firmware_version", nullable = false)
  private String firmwareVersion;

  @Column(name = "previous_firmware_version")
  private String previousFirmwareVersion;

  @Column(name = "firmware_git_sha")
  private String firmwareGitSha;

  @Column(name = "firmware_hash")
  private String firmwareHash;

  @Column(name = "build_time")
  private OffsetDateTime buildTime;

  @Column(name = "hardware_model")
  private String hardwareModel;

  @Column(name = "bootloader_version")
  private String bootloaderVersion;

  @Column(name = "secure_boot_enabled", nullable = false)
  private boolean secureBootEnabled;

  @Column(name = "reported_at", nullable = false)
  private OffsetDateTime reportedAt;

  @Column(name = "received_at_utc", nullable = false)
  private OffsetDateTime receivedAtUtc;

  @Column(name = "source_kind", nullable = false)
  private String sourceKind;

  @Column(name = "source_actor")
  private String sourceActor;

  @Column(name = "source_topic")
  private String sourceTopic;

  @Column(name = "source_partition")
  private Integer sourcePartition;

  @Column(name = "source_offset")
  private Long sourceOffset;

  @Column(name = "updated_at", nullable = false)
  private OffsetDateTime updatedAt;

  public Long getAssetId() {
    return assetId;
  }

  public void setAssetId(Long assetId) {
    this.assetId = assetId;
  }

  public AssetEntity getAsset() {
    return asset;
  }

  public void setAsset(AssetEntity asset) {
    this.asset = asset;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getOtaStatus() {
    return otaStatus;
  }

  public void setOtaStatus(String otaStatus) {
    this.otaStatus = otaStatus;
  }

  public String getFirmwareVersion() {
    return firmwareVersion;
  }

  public void setFirmwareVersion(String firmwareVersion) {
    this.firmwareVersion = firmwareVersion;
  }

  public String getPreviousFirmwareVersion() {
    return previousFirmwareVersion;
  }

  public void setPreviousFirmwareVersion(String previousFirmwareVersion) {
    this.previousFirmwareVersion = previousFirmwareVersion;
  }

  public String getFirmwareGitSha() {
    return firmwareGitSha;
  }

  public void setFirmwareGitSha(String firmwareGitSha) {
    this.firmwareGitSha = firmwareGitSha;
  }

  public String getFirmwareHash() {
    return firmwareHash;
  }

  public void setFirmwareHash(String firmwareHash) {
    this.firmwareHash = firmwareHash;
  }

  public OffsetDateTime getBuildTime() {
    return buildTime;
  }

  public void setBuildTime(OffsetDateTime buildTime) {
    this.buildTime = buildTime;
  }

  public String getHardwareModel() {
    return hardwareModel;
  }

  public void setHardwareModel(String hardwareModel) {
    this.hardwareModel = hardwareModel;
  }

  public String getBootloaderVersion() {
    return bootloaderVersion;
  }

  public void setBootloaderVersion(String bootloaderVersion) {
    this.bootloaderVersion = bootloaderVersion;
  }

  public boolean isSecureBootEnabled() {
    return secureBootEnabled;
  }

  public void setSecureBootEnabled(boolean secureBootEnabled) {
    this.secureBootEnabled = secureBootEnabled;
  }

  public OffsetDateTime getReportedAt() {
    return reportedAt;
  }

  public void setReportedAt(OffsetDateTime reportedAt) {
    this.reportedAt = reportedAt;
  }

  public OffsetDateTime getReceivedAtUtc() {
    return receivedAtUtc;
  }

  public void setReceivedAtUtc(OffsetDateTime receivedAtUtc) {
    this.receivedAtUtc = receivedAtUtc;
  }

  public String getSourceKind() {
    return sourceKind;
  }

  public void setSourceKind(String sourceKind) {
    this.sourceKind = sourceKind;
  }

  public String getSourceActor() {
    return sourceActor;
  }

  public void setSourceActor(String sourceActor) {
    this.sourceActor = sourceActor;
  }

  public String getSourceTopic() {
    return sourceTopic;
  }

  public void setSourceTopic(String sourceTopic) {
    this.sourceTopic = sourceTopic;
  }

  public Integer getSourcePartition() {
    return sourcePartition;
  }

  public void setSourcePartition(Integer sourcePartition) {
    this.sourcePartition = sourcePartition;
  }

  public Long getSourceOffset() {
    return sourceOffset;
  }

  public void setSourceOffset(Long sourceOffset) {
    this.sourceOffset = sourceOffset;
  }

  public OffsetDateTime getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(OffsetDateTime updatedAt) {
    this.updatedAt = updatedAt;
  }
}
