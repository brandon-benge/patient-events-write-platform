package com.patienteventswriteplatform.patient.api;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DeidReadResponse(
    @JsonProperty("de_id") String deId,
    int version,
    @JsonProperty("favorite_color") String favoriteColor,
    @JsonProperty("created_at") String createdAt
) {}
