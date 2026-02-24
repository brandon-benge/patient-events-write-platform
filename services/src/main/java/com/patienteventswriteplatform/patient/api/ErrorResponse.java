package com.patienteventswriteplatform.patient.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponse(
    @JsonProperty("event_id") String eventId,
    String phase,
    @JsonProperty("error_code") String errorCode,
    String message
) {}
