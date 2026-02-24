package com.patienteventswriteplatform.patient.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AdmissionResponse(
    @JsonProperty("event_id") String eventId,
    @JsonProperty("phi_id") String phiId,
    String phase,
    Integer version
) {}
