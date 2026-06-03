package com.combotto.controlplane.services;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import com.combotto.controlplane.api.AssetResponse;
import com.combotto.controlplane.common.ResourceNotFoundException;
import com.combotto.controlplane.model.AssetEntity;
import com.combotto.controlplane.repositories.AssetRepository;

@Service
public class AssetService {

  private final AssetRepository assetRepository;

  public AssetService(AssetRepository assetRepository) {
    this.assetRepository = assetRepository;
  }

  public Page<AssetResponse> list(Pageable pageable) {
    return assetRepository.findAllByDeletedFalse(pageable)
        .map(this::toResponse);
  }

  public AssetResponse getById(Long id) {
    AssetEntity entity = assetRepository.findByIdAndDeletedFalse(id)
        .orElseThrow(() -> new ResourceNotFoundException("Asset not found: " + id));
    return toResponse(entity);
  }

  private AssetResponse toResponse(AssetEntity entity) {
    return new AssetResponse(
        entity.getId(),
        entity.getCompanyId(),
        entity.getAssetType(),
        entity.getName(),
        entity.getExternalRef(),
        entity.getParentAssetId(),
        entity.getSerialNumber(),
        entity.getHardwareModel(),
        entity.getProtocol(),
        entity.getSiteLabel(),
        entity.getMetadataJson(),
        entity.isDeleted());
  }
}
