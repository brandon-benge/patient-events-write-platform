package com.patienteventswriteplatform.patient.domain;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

public record PhiVersionRow(
    UUID phiId,
    UUID deId,
    UUID eventId,
    int version,
    String requestHash,
    String name,
    LocalDate dob,
    String favoriteColor,
    LocalDateTime createdAt
) {}
