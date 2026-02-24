package com.patienteventswriteplatform.patient.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.patienteventswriteplatform.patient.domain.IdempotencyReceipt;
import com.patienteventswriteplatform.patient.domain.Phase;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class IdempotencyService {
  private static final long TTL_SECONDS = 4L * 60L * 60L;

  private final StringRedisTemplate redis;
  private final ObjectMapper mapper;
  private final DefaultRedisScript<Long> casFailedToAdmitted;
  private final DefaultRedisScript<Long> monotonicUpdate;

  public IdempotencyService(StringRedisTemplate redis, ObjectMapper mapper) {
    this.redis = redis;
    this.mapper = mapper;

    this.casFailedToAdmitted = new DefaultRedisScript<>();
    this.casFailedToAdmitted.setResultType(Long.class);
    this.casFailedToAdmitted.setScriptText(
        "local v = redis.call('GET', KEYS[1])\n"
            + "if not v then return -1 end\n"
            + "local o = cjson.decode(v)\n"
            + "if o['request_hash'] ~= ARGV[1] then return -2 end\n"
            + "if o['phase'] ~= 'PHI_FAILED' then return 0 end\n"
            + "o['phase'] = 'ADMITTED'\n"
            + "o['updated_at'] = ARGV[2]\n"
            + "redis.call('SET', KEYS[1], cjson.encode(o), 'KEEPTTL')\n"
            + "return 1");

    this.monotonicUpdate = new DefaultRedisScript<>();
    this.monotonicUpdate.setResultType(Long.class);
    this.monotonicUpdate.setScriptText(
        "local v = redis.call('GET', KEYS[1])\n"
            + "if not v then return -1 end\n"
            + "local o = cjson.decode(v)\n"
            + "if o['request_hash'] ~= ARGV[1] then return -2 end\n"
            + "local new_phase = ARGV[2]\n"
            + "local rank = {ADMITTED=1, PHI_FAILED=2, PHI_REJECTED=2, PHI_COMMITTED=3}\n"
            + "local cur = rank[o['phase']] or 0\n"
            + "local nxt = rank[new_phase] or 0\n"
            + "if nxt < cur then return 0 end\n"
            + "o['phase'] = new_phase\n"
            + "if ARGV[3] ~= '' then o['phi_id'] = ARGV[3] end\n"
            + "if ARGV[4] ~= '' then o['version'] = tonumber(ARGV[4]) end\n"
            + "o['updated_at'] = ARGV[5]\n"
            + "redis.call('SET', KEYS[1], cjson.encode(o), 'KEEPTTL')\n"
            + "return 1");
  }

  public AdmissionDecision admitOrGet(UUID eventId, String requestHash) {
    String key = key(eventId);
    String now = Instant.now().toString();
    IdempotencyReceipt admitted =
        new IdempotencyReceipt(requestHash, Phase.ADMITTED, null, null, now, now, null);

    String value = encode(admitted);
    try {
      Boolean set = redis.opsForValue().setIfAbsent(key, value, java.time.Duration.ofSeconds(TTL_SECONDS));
      if (Boolean.TRUE.equals(set)) {
        return AdmissionDecision.newAdmission();
      }

      IdempotencyReceipt existing = getRequired(eventId);
      if (!requestHash.equals(existing.requestHash())) {
        return AdmissionDecision.hashConflict();
      }

      if (existing.phase() == Phase.PHI_FAILED) {
        Long casResult =
            redis.execute(
                casFailedToAdmitted,
                List.of(key),
                requestHash,
                Instant.now().toString());

        if (casResult != null && casResult == 1L) {
          return AdmissionDecision.retryAdmission();
        }

        return AdmissionDecision.existing(getRequired(eventId));
      }

      return AdmissionDecision.existing(existing);
    } catch (DataAccessException e) {
      throw new IdempotencyUnavailableException("Redis unavailable", e);
    }
  }

  public IdempotencyReceipt markCommitted(UUID eventId, String requestHash, UUID phiId, int version) {
    return markPhase(eventId, requestHash, Phase.PHI_COMMITTED, phiId.toString(), Integer.toString(version));
  }

  public IdempotencyReceipt markRejected(UUID eventId, String requestHash) {
    return markPhase(eventId, requestHash, Phase.PHI_REJECTED, "", "");
  }

  public IdempotencyReceipt markFailed(UUID eventId, String requestHash) {
    return markPhase(eventId, requestHash, Phase.PHI_FAILED, "", "");
  }

  public Optional<IdempotencyReceipt> get(UUID eventId) {
    try {
      String value = redis.opsForValue().get(key(eventId));
      if (value == null) {
        return Optional.empty();
      }
      return Optional.of(decode(value));
    } catch (DataAccessException e) {
      throw new IdempotencyUnavailableException("Redis unavailable", e);
    }
  }

  private IdempotencyReceipt markPhase(
      UUID eventId, String requestHash, Phase phase, String phiId, String version) {
    String key = key(eventId);
    try {
      Long result =
          redis.execute(
              monotonicUpdate,
              List.of(key),
              requestHash,
              phase.name(),
              phiId,
              version,
              Instant.now().toString());

      if (result == null || result < 1) {
        throw new IdempotencyUnavailableException("Unable to update Redis receipt");
      }
      return getRequired(eventId);
    } catch (DataAccessException e) {
      throw new IdempotencyUnavailableException("Redis unavailable", e);
    }
  }

  private IdempotencyReceipt getRequired(UUID eventId) {
    return get(eventId)
        .orElseThrow(
            () ->
                new com.patienteventswriteplatform.patient.error.ApiException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "IDEMPOTENCY_STATE_MISSING",
                    "PHI_FAILED",
                    eventId.toString(),
                    "Idempotency receipt missing"));
  }

  private String key(UUID eventId) {
    return "idempotency:" + eventId;
  }

  private String encode(IdempotencyReceipt receipt) {
    try {
      return mapper.writeValueAsString(receipt);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize receipt", e);
    }
  }

  private IdempotencyReceipt decode(String value) {
    try {
      return mapper.readValue(value, IdempotencyReceipt.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to deserialize receipt", e);
    }
  }

  public record AdmissionDecision(Type type, IdempotencyReceipt receipt) {
    public static AdmissionDecision newAdmission() {
      return new AdmissionDecision(Type.NEW_ADMISSION, null);
    }

    public static AdmissionDecision retryAdmission() {
      return new AdmissionDecision(Type.RETRY_ADMISSION, null);
    }

    public static AdmissionDecision hashConflict() {
      return new AdmissionDecision(Type.HASH_CONFLICT, null);
    }

    public static AdmissionDecision existing(IdempotencyReceipt receipt) {
      return new AdmissionDecision(Type.EXISTING, receipt);
    }
  }

  public enum Type {
    NEW_ADMISSION,
    RETRY_ADMISSION,
    HASH_CONFLICT,
    EXISTING
  }

  public static class IdempotencyUnavailableException extends RuntimeException {
    public IdempotencyUnavailableException(String message) {
      super(message);
    }

    public IdempotencyUnavailableException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
