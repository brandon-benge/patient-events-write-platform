package com.patienteventswriteplatform.patient.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record IdempotencyReceipt(
    @JsonProperty("request_hash") String requestHash,
    Phase phase,
    @JsonProperty("phi_id") String phiId,
    Integer version,
    @JsonProperty("created_at") String createdAt,
    @JsonProperty("updated_at") String updatedAt,
    @JsonProperty("persisted_at") String persistedAt
) {}
