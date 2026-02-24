package com.patienteventswriteplatform.patient.domain;

import java.time.LocalDateTime;
import java.util.UUID;

public record DeidVersionRow(UUID deId, int version, String favoriteColor, LocalDateTime createdAt) {}
