package com.patienteventswriteplatform.patient.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.UUID;

public record WritePatientRequest(
    @NotNull UUID event_id,
    @NotBlank String name,
    @NotBlank @Pattern(regexp = "^\\d{4}-\\d{2}-\\d{2}$") String dob,
    @NotBlank String favorite_color
) {}
