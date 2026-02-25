package com.patienteventswriteplatform.patient;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
abstract class IntegrationTestBase {
  @Container
  static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:16")
          .withDatabaseName("patient_events")
          .withUsername("app")
          .withPassword("app");

  @Container
  static final GenericContainer<?> redis = new GenericContainer<>("redis:7.2-alpine").withExposedPorts(6379);

  @DynamicPropertySource
  static void registerProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", postgres::getJdbcUrl);
    registry.add("spring.datasource.username", postgres::getUsername);
    registry.add("spring.datasource.password", postgres::getPassword);
    registry.add("spring.data.redis.host", redis::getHost);
    registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    registry.add("spring.liquibase.enabled", () -> true);
    registry.add("spring.liquibase.change-log", () -> "file:../infra/liquibase/liquibase.yaml");
  }

  @Autowired protected JdbcTemplate jdbcTemplate;
  @Autowired protected StringRedisTemplate redisTemplate;
  @Autowired protected ObjectMapper objectMapper;

  @BeforeEach
  void cleanState() {
    jdbcTemplate.update("DELETE FROM deid_patient_versions");
    jdbcTemplate.update("DELETE FROM phi_patient_versions");
    jdbcTemplate.update("DELETE FROM phi_patient_head");
    redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
  }

  protected Map<String, Object> createPayload(UUID eventId, String favoriteColor) {
    return Map.of(
        "event_id", eventId.toString(),
        "name", "Jane Doe",
        "dob", "1990-01-01",
        "favorite_color", favoriteColor);
  }

  protected void putFailedReceipt(UUID eventId, String requestHash) throws Exception {
    String now = java.time.Instant.now().toString();
    String json =
        objectMapper.writeValueAsString(
            Map.of(
                "request_hash", requestHash,
                "phase", "PHI_FAILED",
                "created_at", now,
                "updated_at", now));
    redisTemplate
        .opsForValue()
        .set("idempotency:" + eventId, json, Duration.ofHours(4));
  }
}
