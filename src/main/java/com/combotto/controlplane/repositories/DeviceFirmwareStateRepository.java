package com.combotto.controlplane.repositories;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.combotto.controlplane.model.DeviceFirmwareStateEntity;

public interface DeviceFirmwareStateRepository extends JpaRepository<DeviceFirmwareStateEntity, Long> {
  Optional<DeviceFirmwareStateEntity> findByDeviceId(String deviceId);
}
