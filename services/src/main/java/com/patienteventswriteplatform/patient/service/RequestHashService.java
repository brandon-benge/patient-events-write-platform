package com.patienteventswriteplatform.patient.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.patienteventswriteplatform.patient.api.WritePatientRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class RequestHashService {
  private final ObjectMapper mapper;

  public RequestHashService() {
    this.mapper = new ObjectMapper();
    this.mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }

  public String forCreate(WritePatientRequest request) {
    return hash(canonicalPayload("create", null, request));
  }

  public String forUpdate(UUID phiId, WritePatientRequest request) {
    return hash(canonicalPayload("update", phiId, request));
  }

  private String canonicalPayload(String operation, UUID phiId, WritePatientRequest request) {
    ObjectNode node = mapper.createObjectNode();
    node.put("dob", request.dob());
    node.put("event_id", request.event_id().toString());
    node.put("favorite_color", request.favorite_color());
    node.put("name", request.name());
    node.put("operation", operation);
    node.put("phi_id", phiId == null ? "" : phiId.toString());
    try {
      return mapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Unable to canonicalize payload", e);
    }
  }

  private String hash(String canonicalPayload) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(canonicalPayload.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
