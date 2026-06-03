package com.combotto.controlplane.api;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Asset record returned by the control plane")
public record AssetResponse(
    @Schema(description = "Combotto Monitor asset identifier.", example = "7")
    Long id,
    @Schema(description = "Owning Combotto company identifier.", example = "1001")
    Long companyId,
    @Schema(description = "Combotto asset type.", example = "gateway")
    String assetType,
    @Schema(description = "Display name for the asset.", example = "gateway")
    String name,
    @Schema(description = "Combotto external asset reference.", example = "mqtt://192.168.1.91:1883")
    String externalRef,
    @Schema(description = "Parent asset identifier for fleet hierarchy.", example = "7")
    Long parentAssetId,
    @Schema(description = "Device serial number, when known.", example = "B-L475E-IOT01A2")
    String serialNumber,
    @Schema(description = "Hardware model, when known.", example = "STM32L475")
    String hardwareModel,
    @Schema(description = "Protocol used by the asset.", example = "mqtt")
    String protocol,
    @Schema(description = "Human-readable site label.", example = "lab")
    String siteLabel,
    @Schema(description = "Raw metadata JSON maintained by Combotto Monitor.")
    String metadataJson,
    @Schema(description = "Whether Combotto Monitor has soft-deleted this asset.", example = "false")
    boolean deleted) {
}
