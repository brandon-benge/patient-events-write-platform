package com.patienteventswriteplatform.patient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.patienteventswriteplatform.patient.service.IdempotencyService;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.AopTestUtils;

@EnabledIfEnvironmentVariable(named = "RUN_INTEGRATION_TESTS", matches = "true")
class Phase1AtomicCommitReceiptGatingIntegrationTest extends IntegrationTestBase {
  @Autowired private TestRestTemplate restTemplate;

  @SpyBean private IdempotencyService idempotencyService;

  @AfterEach
  void resetSpy() {
    org.mockito.Mockito.reset(AopTestUtils.getUltimateTargetObject(idempotencyService));
  }

  @Test
  void successfulCreateWritesHeadAndVersionAndReceiptIsCommitted() throws Exception {
    UUID eventId = UUID.randomUUID();
    Map<String, Object> payload = createPayload(eventId, "red");

    ResponseEntity<String> response =
        restTemplate.exchange(
            "/api/v1/patient", HttpMethod.POST, new HttpEntity<>(payload), String.class);

    assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
    Map<String, Object> body =
        objectMapper.readValue(response.getBody(), new TypeReference<Map<String, Object>>() {});

    String phiId = (String) body.get("phi_id");
    assertNotNull(phiId);
    assertEquals("PHI_COMMITTED", body.get("phase"));
    assertEquals(1, ((Number) body.get("version")).intValue());

    Integer headRows =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM phi_patient_head WHERE phi_id = ?::uuid AND current_version = 1",
            Integer.class,
            phiId);
    Integer versionRows =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM phi_patient_versions WHERE phi_id = ?::uuid AND version = 1",
            Integer.class,
            phiId);

    assertEquals(1, headRows);
    assertEquals(1, versionRows);

    String receiptJson = redisTemplate.opsForValue().get("idempotency:" + eventId);
    JsonNode receipt = objectMapper.readTree(receiptJson);
    assertEquals("PHI_COMMITTED", receipt.get("phase").asText());
  }

  @Test
  void apiDoesNotReturn202WhenCommittedReceiptWriteFails() throws Exception {
    UUID eventId = UUID.randomUUID();
    Map<String, Object> payload = createPayload(eventId, "purple");

    doThrow(new IdempotencyService.IdempotencyUnavailableException("forced"))
        .when(idempotencyService)
        .markCommitted(eq(eventId), anyString(), any(UUID.class), anyInt());

    ResponseEntity<String> response =
        restTemplate.exchange(
            "/api/v1/patient", HttpMethod.POST, new HttpEntity<>(payload), String.class);

    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());

    Map<String, Object> body =
        objectMapper.readValue(response.getBody(), new TypeReference<Map<String, Object>>() {});
    assertEquals("PHI_FAILED", body.get("phase"));

    Integer versionRows =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM phi_patient_versions WHERE event_id = ?::uuid",
            Integer.class,
            eventId.toString());
    assertEquals(1, versionRows);
  }

  @Test
  void duplicateRequestCannotRegressCommittedReceiptToAdmitted() throws Exception {
    UUID eventId = UUID.randomUUID();
    Map<String, Object> payload = createPayload(eventId, "orange");

    ResponseEntity<String> first =
        restTemplate.exchange(
            "/api/v1/patient", HttpMethod.POST, new HttpEntity<>(payload), String.class);
    assertEquals(HttpStatus.ACCEPTED, first.getStatusCode());

    ResponseEntity<String> second =
        restTemplate.exchange(
            "/api/v1/patient", HttpMethod.POST, new HttpEntity<>(payload), String.class);
    assertEquals(HttpStatus.OK, second.getStatusCode());

    String receiptJson = redisTemplate.opsForValue().get("idempotency:" + eventId);
    JsonNode receipt = objectMapper.readTree(receiptJson);
    assertEquals("PHI_COMMITTED", receipt.get("phase").asText());
  }
}
