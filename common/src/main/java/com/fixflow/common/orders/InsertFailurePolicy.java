package com.fixflow.common.orders;

import java.sql.SQLException;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

/**
 * Decides what a failed insert means for the batch it belongs to.
 * <p>
 * A <em>row-level</em> failure (duplicate key, constraint violation, bad data) is the fault of that one row: the batch
 * falls back to inserting the rows one by one, the failed rows are logged and reported to support, and the batch is
 * acknowledged so that the good rows are not redelivered.
 * <p>
 * A <em>database-level</em> failure (connection lost, database locked or busy, disk full) is not the fault of any
 * row: the batch is aborted without acknowledgment, so that Kafka redelivers it once the database is back.
 */
public final class InsertFailurePolicy {

    private InsertFailurePolicy() {
    }

    /** @return true when only the row is at fault and the batch should fall back to one-by-one inserts. */
    public static boolean isRowLevel(Throwable error) {
        SQLiteException sqlite = findCause(error, SQLiteException.class);
        if (sqlite != null) {
            return isRowLevel(sqlite.getResultCode());
        }
        if (error instanceof DataAccessResourceFailureException
                || error instanceof TransientDataAccessException
                || error instanceof RecoverableDataAccessException) {
            return false;
        }
        if (error instanceof DataIntegrityViolationException || error instanceof NonTransientDataAccessException) {
            return true;
        }
        SQLException sql = findCause(error, SQLException.class);
        if (sql != null && sql.getSQLState() != null) {
            String sqlClass = sql.getSQLState().substring(0, Math.min(2, sql.getSQLState().length()));
            return sqlClass.equals("22") || sqlClass.equals("23"); // data exception, integrity constraint violation
        }
        String message = error.getMessage();
        return message != null && message.contains("SQLITE_CONSTRAINT");
    }

    static boolean isRowLevel(SQLiteErrorCode code) {
        if (code == null) {
            return false;
        }
        return code.name().startsWith("SQLITE_CONSTRAINT")
                || code == SQLiteErrorCode.SQLITE_MISMATCH
                || code == SQLiteErrorCode.SQLITE_TOOBIG
                || code == SQLiteErrorCode.SQLITE_RANGE;
    }

    private static <T extends Throwable> T findCause(Throwable error, Class<T> type) {
        Throwable current = error;
        while (current != null) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return null;
    }
}
