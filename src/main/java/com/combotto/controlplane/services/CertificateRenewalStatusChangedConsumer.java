package com.combotto.controlplane.services;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.combotto.controlplane.api.CertificateRenewalStatusChangedEvent;
import com.combotto.controlplane.model.CertificateRenewalStatusHistoryEntity;
import com.combotto.controlplane.repositories.CertificateRenewalStatusHistoryRepository;

@Component
public class CertificateRenewalStatusChangedConsumer {

  private static final Logger log = LoggerFactory.getLogger(CertificateRenewalStatusChangedConsumer.class);

  private final CertificateRenewalStatusHistoryRepository historyRepository;

  public CertificateRenewalStatusChangedConsumer(
      CertificateRenewalStatusHistoryRepository historyRepository) {
    this.historyRepository = historyRepository;
  }

  @KafkaListener(topics = "${audit.kafka.topics.certificate.renew-status-changed:certificate-renewal-status-changed}", groupId = "${spring.kafka.consumer.certificate-renewal-status-history-group-id:control-plane-renewal-history}", containerFactory = "stringKafkaListenerContainerFactory")
  public void handle(CertificateRenewalStatusChangedEvent event) {
    log.info("Consumed renewal status change event for certificate {}", event.certificateId());
    CertificateRenewalStatusHistoryEntity history = new CertificateRenewalStatusHistoryEntity();

    history.setId(UUID.randomUUID());
    history.setCertificateId(event.certificateId());
    history.setTenantId(event.tenantId());
    history.setOldRenewalStatus(event.oldRenewalStatus());
    history.setNewRenewalStatus(event.newRenewalStatus());
    history.setBlockedReason(event.blockedReason());
    history.setUpdatedBy(event.updatedBy());
    history.setOccurredAt(event.occurredAt());
    history.setCreatedAt(OffsetDateTime.now().truncatedTo(ChronoUnit.MILLIS));

    historyRepository.save(history);
  }
}
