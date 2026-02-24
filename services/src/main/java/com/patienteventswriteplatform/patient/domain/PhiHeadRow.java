package com.patienteventswriteplatform.patient.domain;

import java.util.UUID;

public record PhiHeadRow(UUID phiId, UUID deId, int currentVersion, int persistedVersion) {}
