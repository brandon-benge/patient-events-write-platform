package com.patienteventswriteplatform.patient.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PhiReadResponse(
    @JsonProperty("phi_id") String phiId,
    @JsonProperty("de_id") String deId,
    @JsonProperty("event_id") String eventId,
    int version,
    @JsonProperty("request_hash") String requestHash,
    String name,
    String dob,
    @JsonProperty("favorite_color") String favoriteColor,
    @JsonProperty("created_at") String createdAt
) {}
