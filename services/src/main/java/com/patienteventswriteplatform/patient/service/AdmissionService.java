package com.patienteventswriteplatform.patient.service;

import com.patienteventswriteplatform.patient.api.AdmissionResponse;
import com.patienteventswriteplatform.patient.api.WritePatientRequest;
import com.patienteventswriteplatform.patient.domain.IdempotencyReceipt;
import com.patienteventswriteplatform.patient.domain.Phase;
import com.patienteventswriteplatform.patient.error.ApiException;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class AdmissionService {
  private final IdempotencyService idempotencyService;
  private final PhiCommitService phiCommitService;
  private final RequestHashService requestHashService;

  public AdmissionService(
      IdempotencyService idempotencyService,
      PhiCommitService phiCommitService,
      RequestHashService requestHashService) {
    this.idempotencyService = idempotencyService;
    this.phiCommitService = phiCommitService;
    this.requestHashService = requestHashService;
  }

  public AdmissionOutcome create(WritePatientRequest request) {
    String requestHash = requestHashService.forCreate(request);
    return processAdmission(request, requestHash, true, null);
  }

  public AdmissionOutcome update(UUID phiId, WritePatientRequest request) {
    String requestHash = requestHashService.forUpdate(phiId, request);
    return processAdmission(request, requestHash, false, phiId);
  }

  private AdmissionOutcome processAdmission(
      WritePatientRequest request, String requestHash, boolean create, UUID phiId) {
    IdempotencyService.AdmissionDecision decision;
    try {
      decision = idempotencyService.admitOrGet(request.event_id(), requestHash);
    } catch (IdempotencyService.IdempotencyUnavailableException e) {
      throw new ApiException(
          HttpStatus.SERVICE_UNAVAILABLE,
          "IDEMPOTENCY_UNAVAILABLE",
          "PHI_FAILED",
          request.event_id().toString(),
          "Redis unavailable; admission failed closed");
    }

    if (decision.type() == IdempotencyService.Type.HASH_CONFLICT) {
      throw new ApiException(
          HttpStatus.CONFLICT,
          "EVENT_HASH_MISMATCH",
          "PHI_REJECTED",
          request.event_id().toString(),
          "event_id reused with a different payload");
    }

    if (decision.type() == IdempotencyService.Type.EXISTING) {
      return AdmissionOutcome.duplicate(toResponse(request.event_id(), decision.receipt()));
    }

    PhiCommitService.CommitResult commitResult;
    try {
      commitResult = create ? phiCommitService.commitCreate(request, requestHash) : phiCommitService.commitUpdate(phiId, request, requestHash);
    } catch (ApiException e) {
      if ("PHI_REJECTED".equals(e.getPhase())) {
        try {
          idempotencyService.markRejected(request.event_id(), requestHash);
        } catch (Exception ignored) {
          // Best effort status update.
        }
      } else {
        try {
          idempotencyService.markFailed(request.event_id(), requestHash);
        } catch (Exception ignored) {
          // Best effort status update.
        }
      }
      throw e;
    }

    try {
      IdempotencyReceipt receipt =
          idempotencyService.markCommitted(
              request.event_id(), requestHash, commitResult.phiId(), commitResult.version());
      return AdmissionOutcome.accepted(toResponse(request.event_id(), receipt));
    } catch (IdempotencyService.IdempotencyUnavailableException e) {
      throw new ApiException(
          HttpStatus.SERVICE_UNAVAILABLE,
          "IDEMPOTENCY_COMMIT_RECEIPT_UNAVAILABLE",
          "PHI_FAILED",
          request.event_id().toString(),
          "PHI committed but receipt update failed");
    }
  }

  private AdmissionResponse toResponse(UUID eventId, IdempotencyReceipt receipt) {
    return new AdmissionResponse(
        eventId.toString(), receipt.phiId(), receipt.phase().name(), receipt.version());
  }

  public record AdmissionOutcome(HttpStatus status, AdmissionResponse response) {
    public static AdmissionOutcome accepted(AdmissionResponse response) {
      return new AdmissionOutcome(HttpStatus.ACCEPTED, response);
    }

    public static AdmissionOutcome duplicate(AdmissionResponse response) {
      return new AdmissionOutcome(HttpStatus.OK, response);
    }
  }
}
