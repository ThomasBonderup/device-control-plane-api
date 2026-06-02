package com.combotto.controlplane.repositories;

import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.combotto.controlplane.model.AssetEntity;

public interface AssetRepository extends JpaRepository<AssetEntity, Long> {
  Optional<AssetEntity> findByIdAndDeletedFalse(Long id);

  boolean existsByIdAndDeletedFalse(Long id);

  Page<AssetEntity> findAllByDeletedFalse(Pageable pageable);

  default Optional<AssetEntity> findFirstActiveByDeviceIdentifier(String deviceIdentifier) {
    return findActiveByDeviceIdentifier(deviceIdentifier, PageRequest.of(0, 1)).stream().findFirst();
  }

  @Query("""
      select asset
      from AssetEntity asset
      where asset.deleted = false
        and (asset.serialNumber = :deviceIdentifier or asset.externalRef = :deviceIdentifier)
      order by asset.id asc
      """)
  Page<AssetEntity> findActiveByDeviceIdentifier(
      @Param("deviceIdentifier") String deviceIdentifier,
      Pageable pageable);
}
