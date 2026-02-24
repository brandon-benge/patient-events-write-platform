package com.patienteventswriteplatform.patient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.patienteventswriteplatform.patient.api.WritePatientRequest;
import com.patienteventswriteplatform.patient.service.RequestHashService;
import java.util.UUID;
import org.junit.jupiter.api.Test;

// unit tests
class RequestHashServiceTest {
  private final RequestHashService service = new RequestHashService();

  @Test
  void createHashIsDeterministic() {
    UUID eventId = UUID.randomUUID();
    WritePatientRequest req = new WritePatientRequest(eventId, "Jane Doe", "1990-01-01", "red");

    String h1 = service.forCreate(req);
    String h2 = service.forCreate(req);

    assertEquals(h1, h2);
  }

  @Test
  void updateHashIncludesPhiId() {
    UUID eventId = UUID.randomUUID();
    UUID phiA = UUID.randomUUID();
    UUID phiB = UUID.randomUUID();
    WritePatientRequest req = new WritePatientRequest(eventId, "Jane Doe", "1990-01-01", "red");

    String h1 = service.forUpdate(phiA, req);
    String h2 = service.forUpdate(phiB, req);

    assertNotEquals(h1, h2);
  }
  @Test
  void createHashChangesWhenPayloadChanges() {
    UUID eventId = UUID.randomUUID();
    WritePatientRequest req1 = new WritePatientRequest(eventId, "Jane Doe", "1990-01-01", "red");
    WritePatientRequest req2 = new WritePatientRequest(eventId, "Jane Doe", "1990-01-01", "blue");

    String h1 = service.forCreate(req1);
    String h2 = service.forCreate(req2);

    assertNotEquals(h1, h2);
  }
}
