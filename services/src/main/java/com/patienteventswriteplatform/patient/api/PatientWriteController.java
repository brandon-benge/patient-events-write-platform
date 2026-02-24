package com.patienteventswriteplatform.patient.api;

import com.patienteventswriteplatform.patient.service.AdmissionService;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/patient")
public class PatientWriteController {
  private final AdmissionService admissionService;

  public PatientWriteController(AdmissionService admissionService) {
    this.admissionService = admissionService;
  }

  @PostMapping
  public ResponseEntity<AdmissionResponse> create(@Valid @RequestBody WritePatientRequest request) {
    AdmissionService.AdmissionOutcome outcome = admissionService.create(request);
    return ResponseEntity.status(outcome.status()).body(outcome.response());
  }

  @PutMapping("/{phi_id}")
  public ResponseEntity<AdmissionResponse> update(
      @PathVariable("phi_id") UUID phiId, @Valid @RequestBody WritePatientRequest request) {
    AdmissionService.AdmissionOutcome outcome = admissionService.update(phiId, request);
    return ResponseEntity.status(outcome.status()).body(outcome.response());
  }
}
