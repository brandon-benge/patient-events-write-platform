package com.patienteventswriteplatform.patient.repo;

import com.patienteventswriteplatform.patient.api.WritePatientRequest;
import com.patienteventswriteplatform.patient.domain.DeidVersionRow;
import com.patienteventswriteplatform.patient.domain.PhiHeadRow;
import com.patienteventswriteplatform.patient.domain.PhiVersionRow;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class PhiRepository {
  private final JdbcTemplate jdbc;

  private static final RowMapper<PhiHeadRow> HEAD_MAPPER =
      (rs, rowNum) ->
          new PhiHeadRow(
              rs.getObject("phi_id", UUID.class),
              rs.getObject("de_id", UUID.class),
              rs.getInt("current_version"),
              rs.getInt("persisted_version"));

  private static final RowMapper<DeidVersionRow> DEID_MAPPER =
      (rs, rowNum) ->
          new DeidVersionRow(
              rs.getObject("de_id", UUID.class),
              rs.getInt("version"),
              rs.getString("favorite_color"),
              rs.getObject("created_at", java.time.LocalDateTime.class));

  private static final RowMapper<PhiVersionRow> PHI_MAPPER =
      (rs, rowNum) ->
          new PhiVersionRow(
              rs.getObject("phi_id", UUID.class),
              rs.getObject("de_id", UUID.class),
              rs.getObject("event_id", UUID.class),
              rs.getInt("version"),
              rs.getString("request_hash"),
              rs.getString("name"),
              rs.getObject("dob", LocalDate.class),
              rs.getString("favorite_color"),
              rs.getObject("created_at", java.time.LocalDateTime.class));

  public PhiRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public void insertHead(UUID phiId, UUID deId) {
    jdbc.update(
        """
        INSERT INTO phi_patient_head (phi_id, de_id, current_version, persisted_version, last_updated_at)
        VALUES (?, ?, 1, 0, NOW())
        """,
        phiId,
        deId);
  }

  public int insertPhiVersion(
      UUID phiId,
      UUID deId,
      UUID eventId,
      int version,
      String requestHash,
      WritePatientRequest request) {
    return jdbc.update(
        """
        INSERT INTO phi_patient_versions (phi_id, de_id, event_id, request_hash, version, name, dob, favorite_color)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        phiId,
        deId,
        eventId,
        requestHash,
        version,
        request.name(),
        Date.valueOf(request.dob()),
        request.favorite_color());
  }

  public Optional<PhiHeadRow> selectHeadForUpdate(UUID phiId) {
    List<PhiHeadRow> rows =
        jdbc.query(
            """
            SELECT phi_id, de_id, current_version, persisted_version
            FROM phi_patient_head
            WHERE phi_id = ?
            FOR UPDATE
            """,
            HEAD_MAPPER,
            phiId);
    return rows.stream().findFirst();
  }

  public int updateCurrentVersion(UUID phiId, int version) {
    return jdbc.update(
        "UPDATE phi_patient_head SET current_version = ?, last_updated_at = NOW() WHERE phi_id = ?",
        version,
        phiId);
  }

  public Optional<PhiHeadRow> findHead(UUID phiId) {
    List<PhiHeadRow> rows =
        jdbc.query(
            """
            SELECT phi_id, de_id, current_version, persisted_version
            FROM phi_patient_head
            WHERE phi_id = ?
            """,
            HEAD_MAPPER,
            phiId);
    return rows.stream().findFirst();
  }

  public Optional<DeidVersionRow> findLatestDeidByDeId(UUID deId) {
    List<DeidVersionRow> rows =
        jdbc.query(
            """
            SELECT de_id, version, favorite_color, created_at
            FROM deid_patient_versions
            WHERE de_id = ?
            ORDER BY version DESC
            LIMIT 1
            """,
            DEID_MAPPER,
            deId);
    return rows.stream().findFirst();
  }

  public Optional<DeidVersionRow> findDeidByKey(UUID deId, int version) {
    List<DeidVersionRow> rows =
        jdbc.query(
            """
            SELECT de_id, version, favorite_color, created_at
            FROM deid_patient_versions
            WHERE de_id = ? AND version = ?
            """,
            DEID_MAPPER,
            deId,
            version);
    return rows.stream().findFirst();
  }

  public Optional<PhiVersionRow> findLatestPhiByPhiId(UUID phiId) {
    List<PhiVersionRow> rows =
        jdbc.query(
            """
            SELECT phi_id, de_id, event_id, request_hash, version, name, dob, favorite_color, created_at
            FROM phi_patient_versions
            WHERE phi_id = ?
            ORDER BY version DESC
            LIMIT 1
            """,
            PHI_MAPPER,
            phiId);
    return rows.stream().findFirst();
  }

  public Optional<PhiVersionRow> findPhiByDeIdAndVersion(UUID deId, int version) {
    List<PhiVersionRow> rows =
        jdbc.query(
            """
            SELECT phi_id, de_id, event_id, request_hash, version, name, dob, favorite_color, created_at
            FROM phi_patient_versions
            WHERE de_id = ? AND version = ?
            LIMIT 1
            """,
            PHI_MAPPER,
            deId,
            version);
    return rows.stream().findFirst();
  }

  public Optional<PhiVersionRow> findLatestPhiByDeId(UUID deId) {
    List<PhiVersionRow> rows =
        jdbc.query(
            """
            SELECT phi_id, de_id, event_id, request_hash, version, name, dob, favorite_color, created_at
            FROM phi_patient_versions
            WHERE de_id = ?
            ORDER BY version DESC
            LIMIT 1
            """,
            PHI_MAPPER,
            deId);
    return rows.stream().findFirst();
  }

  public boolean isConstraintViolation(DataAccessException e) {
    Throwable root = rootCause(e);
    if (root instanceof org.postgresql.util.PSQLException pgEx) {
      String sqlState = pgEx.getSQLState();
      return sqlState != null && ("23505".equals(sqlState) || sqlState.startsWith("23"));
    }
    return false;
  }

  private Throwable rootCause(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    return cur;
  }
}
