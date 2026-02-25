package com.patienteventswriteplatform.patient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.patienteventswriteplatform.patient.service.RequestHashService;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = "true")
class AdmissionIdempotencyIntegrationTest extends IntegrationTestBase {
  @Autowired private TestRestTemplate restTemplate;
  @Autowired private RequestHashService requestHashService;

  @Test
  void duplicateEventSamePayloadReturns200AndNoSecondCommit() {
    UUID eventId = UUID.randomUUID();
    Map<String, Object> payload = createPayload(eventId, "red");

    ResponseEntity<String> first =
        restTemplate.exchange(
            "/api/v1/patient", HttpMethod.POST, new HttpEntity<>(payload), String.class);
    assertEquals(HttpStatus.ACCEPTED, first.getStatusCode());

    ResponseEntity<String> second =
        restTemplate.exchange(
            "/api/v1/patient", HttpMethod.POST, new HttpEntity<>(payload), String.class);
    assertEquals(HttpStatus.OK, second.getStatusCode());

    Integer versions =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM phi_patient_versions WHERE event_id = ?::uuid",
            Integer.class,
            eventId.toString());
    assertEquals(1, versions);
  }

  @Test
  void duplicateEventDifferentPayloadReturns409AndDoesNotMutatePhi() throws Exception {
    UUID eventId = UUID.randomUUID();
    Map<String, Object> firstPayload = createPayload(eventId, "red");
    Map<String, Object> secondPayload = createPayload(eventId, "blue");

    ResponseEntity<String> first =
        restTemplate.exchange(
            "/api/v1/patient", HttpMethod.POST, new HttpEntity<>(firstPayload), String.class);
    assertEquals(HttpStatus.ACCEPTED, first.getStatusCode());

    ResponseEntity<String> second =
        restTemplate.exchange(
            "/api/v1/patient", HttpMethod.POST, new HttpEntity<>(secondPayload), String.class);
    assertEquals(HttpStatus.CONFLICT, second.getStatusCode());

    Map<String, Object> error =
        objectMapper.readValue(second.getBody(), new TypeReference<Map<String, Object>>() {});
    assertEquals("PHI_REJECTED", error.get("phase"));

    Integer versions =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM phi_patient_versions WHERE event_id = ?::uuid",
            Integer.class,
            eventId.toString());
    assertEquals(1, versions);
  }

  @Test
  void phiFailedReceiptWithMatchingHashRetriesAndReturns202() throws Exception {
    UUID eventId = UUID.randomUUID();
    Map<String, Object> payload = createPayload(eventId, "green");
    String requestHash =
        requestHashService.forCreate(
            new com.patienteventswriteplatform.patient.api.WritePatientRequest(
                eventId, "Jane Doe", "1990-01-01", "green"));

    putFailedReceipt(eventId, requestHash);

    ResponseEntity<String> response =
        restTemplate.exchange(
            "/api/v1/patient", HttpMethod.POST, new HttpEntity<>(payload), String.class);
    assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());

    Map<String, Object> body =
        objectMapper.readValue(response.getBody(), new TypeReference<Map<String, Object>>() {});
    assertEquals("PHI_COMMITTED", body.get("phase"));
    assertNotNull(body.get("phi_id"));

    Integer versions =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM phi_patient_versions WHERE event_id = ?::uuid",
            Integer.class,
            eventId.toString());
    assertEquals(1, versions);
  }
}
