package com.patienteventswriteplatform.patient.error;

import org.springframework.http.HttpStatus;

public class ApiException extends RuntimeException {
  private final HttpStatus status;
  private final String errorCode;
  private final String phase;
  private final String eventId;

  public ApiException(HttpStatus status, String errorCode, String phase, String eventId, String message) {
    super(message);
    this.status = status;
    this.errorCode = errorCode;
    this.phase = phase;
    this.eventId = eventId;
  }

  public HttpStatus getStatus() {
    return status;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public String getPhase() {
    return phase;
  }

  public String getEventId() {
    return eventId;
  }
}
