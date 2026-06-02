package com.combotto.controlplane.repositories;

import java.util.List;
import java.util.UUID;
import java.time.OffsetDateTime;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.combotto.controlplane.model.CertificateEntity;
import com.combotto.controlplane.model.CertificateStatus;
import com.combotto.controlplane.model.RenewalStatus;

public interface CertificateRepository extends JpaRepository<CertificateEntity, UUID> {

  java.util.Optional<CertificateEntity> findByIdAndTenantId(UUID id, String tenantId);

  boolean existsByIdAndTenantId(UUID id, String tenantId);

  @Query("""
          select c
          from CertificateEntity c
          where (:tenantId is null or c.tenantId = :tenantId)
            and (:status is null or c.status = :status)
            and (:renewalStatus is null or c.renewalStatus = :renewalStatus)
      """)
  Page<CertificateEntity> findByFilters(
      @Param("tenantId") String tenantId,
      @Param("status") CertificateStatus status,
      @Param("renewalStatus") RenewalStatus renewalStatus,
      Pageable pageable);

  @Query(value = """
      select c
      from CertificateEntity c
      where c.notAfter is not null
        and c.notAfter > :now
        and c.notAfter <= :threshold
        and (:tenantId is null or c.tenantId = :tenantId)
        and (:owner is null or c.owner = :owner)
        and (:renewalStatus is null or c.renewalStatus = :renewalStatus)
      """, countQuery = """
      select count(c)
      from CertificateEntity c
      where c.notAfter is not null
        and c.notAfter > :now
        and c.notAfter <= :threshold
        and (:tenantId is null or c.tenantId = :tenantId)
        and (:owner is null or c.owner = :owner)
        and (:renewalStatus is null or c.renewalStatus = :renewalStatus)
      """)
  Page<CertificateEntity> findExpiringSoonByFilters(
      @Param("now") OffsetDateTime now,
      @Param("threshold") OffsetDateTime threshold,
      @Param("tenantId") String tenantId,
      @Param("owner") String owner,
      @Param("renewalStatus") RenewalStatus renewalStatus,
      Pageable pageable);

  @Query(value = """
      select c
      from CertificateEntity c
      where
        c.tenantId = :tenantId
        and (
          (c.notAfter is not null and c.notAfter <= :now)
        or
        (c.renewalStatus = :renewalBlocked)
        or
        (
          c.notAfter is not null
          and c.notAfter > :now
          and c.notAfter <= :threshold
          and (
            c.owner is null
            or trim(c.owner) = ''
            or c.renewalStatus = :renewalNotStarted
            or c.renewalStatus = :renewalPlanned
            or c.renewalStatus = :renewalInProgress
          )
        )
      )
      """, countQuery = """
      select count(c)
      from CertificateEntity c
      where
        c.tenantId = :tenantId
        and (
          (c.notAfter is not null and c.notAfter <= :now)
        or
        (c.renewalStatus = :renewalBlocked)
        or
        (
          c.notAfter is not null
          and c.notAfter > :now
          and c.notAfter <= :threshold
          and (
            c.owner is null
            or trim(c.owner) = ''
            or c.renewalStatus = :renewalNotStarted
            or c.renewalStatus = :renewalPlanned
            or c.renewalStatus = :renewalInProgress
          )
        )
      )
      """)
  Page<CertificateEntity> findAttentionNeeded(
      @Param("now") OffsetDateTime now,
      @Param("threshold") OffsetDateTime threshold,
      @Param("tenantId") String tenantId,
      @Param("renewalNotStarted") RenewalStatus renewalNotStarted,
      @Param("renewalPlanned") RenewalStatus renewalPlanned,
      @Param("renewalInProgress") RenewalStatus renewalInProgress,
      @Param("renewalBlocked") RenewalStatus renewalBlocked,
      Pageable pageable);

  long countByTenantId(String tenantId);

  long countByTenantIdAndStatus(String tenantId, CertificateStatus status);

  @Query("""
      select count(c)
      from CertificateEntity c
      where c.notAfter is not null
        and c.notAfter > :now
        and c.notAfter <= :threshold
        and c.tenantId = :tenantId
      """)
  long countExpiringSoon(
      @Param("now") OffsetDateTime now,
      @Param("tenantId") String tenantId,
      @Param("threshold") OffsetDateTime threshold);

  long countByTenantIdAndRenewalStatus(String tenantId, RenewalStatus renewalStatus);

  @Query("""
      select c.status as status, c.renewalStatus as renewalStatus, count(c) as count
      from CertificateEntity c
      group by c.status, c.renewalStatus
      """)
  List<CertificateStatusRenewalCount> countByStatusAndRenewalStatus();

  @Query("""
      select count(c)
      from CertificateEntity c
      where c.notAfter is not null
        and c.notAfter > :now
        and c.notAfter <= :threshold
      """)
  long countExpiringSoon(
      @Param("now") OffsetDateTime now,
      @Param("threshold") OffsetDateTime threshold);

  long countByStatus(CertificateStatus status);

  long countByRenewalStatus(RenewalStatus renewalStatus);

  @Query("""
      select count(c)
      from CertificateEntity c
      where not exists (
        select b.id
        from CertificateBindingEntity b
        where b.certificate = c
      )
      """)
  long countUnbound();

  @Query("""
      select min(c.notAfter)
      from CertificateEntity c
      where c.notAfter is not null
      """)
  OffsetDateTime findNextExpiry();
}
