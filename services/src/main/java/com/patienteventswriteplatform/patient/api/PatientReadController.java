package com.patienteventswriteplatform.patient.api;

import com.patienteventswriteplatform.patient.domain.DeidVersionRow;
import com.patienteventswriteplatform.patient.domain.PhiVersionRow;
import com.patienteventswriteplatform.patient.service.ReadService;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class PatientReadController {
  private final ReadService readService;

  public PatientReadController(ReadService readService) {
    this.readService = readService;
  }

  @GetMapping("/deid/patients/by-deid/{de_id}")
  public ResponseEntity<DeidReadResponse> getDeidByDeId(@PathVariable("de_id") UUID deId) {
    DeidVersionRow row = readService.getDeidByDeId(deId);
    if (row == null) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(toDeidResponse(row));
  }

  @GetMapping("/deid/patients/by-phi_id/{phi_id}")
  public ResponseEntity<DeidReadResponse> getDeidByPhiId(@PathVariable("phi_id") UUID phiId) {
    DeidVersionRow row = readService.getDeidByPhiId(phiId);
    if (row == null) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(toDeidResponse(row));
  }

  @GetMapping("/phi/patients/by-deid/{de_id}")
  public ResponseEntity<PhiReadResponse> getPhiByDeId(
      @PathVariable("de_id") UUID deId,
      @RequestParam(name = "version", defaultValue = "latest") String version) {
    PhiVersionRow row = readService.getPhiByDeId(deId, version);
    if (row == null) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(toPhiResponse(row));
  }

  @GetMapping("/phi/patients/by-phi_id/{phi_id}")
  public ResponseEntity<PhiReadResponse> getPhiByPhiId(@PathVariable("phi_id") UUID phiId) {
    PhiVersionRow row = readService.getPhiByPhiId(phiId);
    if (row == null) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(toPhiResponse(row));
  }

  private DeidReadResponse toDeidResponse(DeidVersionRow row) {
    return new DeidReadResponse(
        row.deId().toString(), row.version(), row.favoriteColor(), row.createdAt().toString());
  }

  private PhiReadResponse toPhiResponse(PhiVersionRow row) {
    return new PhiReadResponse(
        row.phiId().toString(),
        row.deId().toString(),
        row.eventId().toString(),
        row.version(),
        row.requestHash(),
        row.name(),
        row.dob() == null ? null : row.dob().toString(),
        row.favoriteColor(),
        row.createdAt().toString());
  }
}
