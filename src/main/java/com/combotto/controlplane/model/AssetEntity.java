package com.combotto.controlplane.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "assets", schema = "public")
public class AssetEntity {

  @Id
  private Long id;

  @Column(name = "company_id", nullable = false)
  private Long companyId;

  @Column(name = "asset_type", nullable = false)
  private String assetType;

  @Column(nullable = false)
  private String name;

  @Column(name = "external_ref", nullable = false)
  private String externalRef;

  @Column(name = "parent_asset_id")
  private Long parentAssetId;

  @Column(name = "serial_number")
  private String serialNumber;

  @Column(name = "hardware_model")
  private String hardwareModel;

  private String protocol;

  @Column(name = "site_label")
  private String siteLabel;

  @Column(name = "metadata_json")
  private String metadataJson;

  @Column(name = "is_deleted", nullable = false)
  private boolean deleted;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getCompanyId() {
    return companyId;
  }

  public void setCompanyId(Long companyId) {
    this.companyId = companyId;
  }

  public String getAssetType() {
    return assetType;
  }

  public void setAssetType(String assetType) {
    this.assetType = assetType;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getExternalRef() {
    return externalRef;
  }

  public void setExternalRef(String externalRef) {
    this.externalRef = externalRef;
  }

  public Long getParentAssetId() {
    return parentAssetId;
  }

  public void setParentAssetId(Long parentAssetId) {
    this.parentAssetId = parentAssetId;
  }

  public String getSerialNumber() {
    return serialNumber;
  }

  public void setSerialNumber(String serialNumber) {
    this.serialNumber = serialNumber;
  }

  public String getHardwareModel() {
    return hardwareModel;
  }

  public void setHardwareModel(String hardwareModel) {
    this.hardwareModel = hardwareModel;
  }

  public String getProtocol() {
    return protocol;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public String getSiteLabel() {
    return siteLabel;
  }

  public void setSiteLabel(String siteLabel) {
    this.siteLabel = siteLabel;
  }

  public String getMetadataJson() {
    return metadataJson;
  }

  public void setMetadataJson(String metadataJson) {
    this.metadataJson = metadataJson;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }
}
