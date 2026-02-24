package com.patienteventswriteplatform.patient.service;

import com.patienteventswriteplatform.patient.api.WritePatientRequest;
import com.patienteventswriteplatform.patient.domain.PhiHeadRow;
import com.patienteventswriteplatform.patient.error.ApiException;
import com.patienteventswriteplatform.patient.repo.PhiRepository;
import java.util.UUID;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class PhiCommitService {
  private static final int MAX_CREATE_RETRIES = 5;

  private final PhiRepository repo;
  private final TransactionTemplate txTemplate;

  public PhiCommitService(PhiRepository repo, PlatformTransactionManager txManager) {
    this.repo = repo;
    this.txTemplate = new TransactionTemplate(txManager);
  }

  public CommitResult commitCreate(WritePatientRequest request, String requestHash) {
    int tries = 0;
    while (tries < MAX_CREATE_RETRIES) {
      tries++;
      UUID phiId = UUID.randomUUID();
      UUID deId = UUID.randomUUID();
      try {
        CommitResult result =
            txTemplate.execute(
                status -> {
                  repo.insertHead(phiId, deId);
                  repo.insertPhiVersion(phiId, deId, request.event_id(), 1, requestHash, request);
                  return new CommitResult(phiId, 1);
                });
        if (result != null) {
          return result;
        }
      } catch (DataAccessException e) {
        if (repo.isConstraintViolation(e)) {
          continue;
        }
        throw classifyDataAccess(request.event_id(), e);
      }
    }
    throw new ApiException(
        HttpStatus.SERVICE_UNAVAILABLE,
        "UUID_COLLISION_RETRY_EXHAUSTED",
        "PHI_FAILED",
        request.event_id().toString(),
        "Could not allocate unique identifiers");
  }

  public CommitResult commitUpdate(UUID phiId, WritePatientRequest request, String requestHash) {
    CommitResult result =
        txTemplate.execute(
            status -> {
              try {
                PhiHeadRow head =
                    repo.selectHeadForUpdate(phiId)
                        .orElseThrow(
                            () ->
                                new ApiException(
                                    HttpStatus.NOT_FOUND,
                                    "PHI_NOT_FOUND",
                                    "PHI_REJECTED",
                                    request.event_id().toString(),
                                    "phi_id not found"));

                int nextVersion = head.currentVersion() + 1;
                repo.updateCurrentVersion(phiId, nextVersion);
                repo.insertPhiVersion(
                    phiId, head.deId(), request.event_id(), nextVersion, requestHash, request);
                return new CommitResult(phiId, nextVersion);
              } catch (DataAccessException e) {
                throw classifyDataAccess(request.event_id(), e);
              }
            });

    if (result == null) {
      throw new ApiException(
          HttpStatus.SERVICE_UNAVAILABLE,
          "PHI_STORE_UNAVAILABLE",
          "PHI_FAILED",
          request.event_id().toString(),
          "PHI store unavailable");
    }
    return result;
  }

  private ApiException classifyDataAccess(UUID eventId, DataAccessException e) {
    if (e instanceof CannotAcquireLockException || e instanceof TransientDataAccessException) {
      return new ApiException(
          HttpStatus.SERVICE_UNAVAILABLE,
          "PHI_TRANSIENT_FAILURE",
          "PHI_FAILED",
          eventId.toString(),
          "Transient PHI store failure");
    }

    if (repo.isConstraintViolation(e)) {
      return new ApiException(
          HttpStatus.UNPROCESSABLE_ENTITY,
          "PHI_CONSTRAINT_REJECTED",
          "PHI_REJECTED",
          eventId.toString(),
          "PHI constraint rejected the request");
    }

    return new ApiException(
        HttpStatus.SERVICE_UNAVAILABLE,
        "PHI_STORE_UNAVAILABLE",
        "PHI_FAILED",
        eventId.toString(),
        "PHI store unavailable");
  }

  public record CommitResult(UUID phiId, int version) {}
}
