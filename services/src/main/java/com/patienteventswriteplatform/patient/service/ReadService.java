package com.patienteventswriteplatform.patient.service;

import com.patienteventswriteplatform.patient.domain.DeidVersionRow;
import com.patienteventswriteplatform.patient.domain.PhiHeadRow;
import com.patienteventswriteplatform.patient.domain.PhiVersionRow;
import com.patienteventswriteplatform.patient.error.ApiException;
import com.patienteventswriteplatform.patient.repo.PhiRepository;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class ReadService {
  private final PhiRepository repo;

  public ReadService(PhiRepository repo) {
    this.repo = repo;
  }

  public DeidVersionRow getDeidByDeId(UUID deId) {
    return repo.findLatestDeidByDeId(deId).orElse(null);
  }

  public DeidVersionRow getDeidByPhiId(UUID phiId) {
    PhiHeadRow head = repo.findHead(phiId).orElse(null);
    if (head == null || head.persistedVersion() <= 0) {
      return null;
    }
    return repo.findDeidByKey(head.deId(), head.persistedVersion()).orElse(null);
  }

  public PhiVersionRow getPhiByDeId(UUID deId, String version) {
    if ("latest".equalsIgnoreCase(version)) {
      return repo.findLatestPhiByDeId(deId).orElse(null);
    }
    int v;
    try {
      v = Integer.parseInt(version);
    } catch (NumberFormatException e) {
      throw new ApiException(
          HttpStatus.BAD_REQUEST,
          "INVALID_VERSION",
          "PHI_REJECTED",
          null,
          "version must be an integer or 'latest'");
    }
    return repo.findPhiByDeIdAndVersion(deId, v).orElse(null);
  }

  public PhiVersionRow getPhiByPhiId(UUID phiId) {
    return repo.findLatestPhiByPhiId(phiId).orElse(null);
  }
}
