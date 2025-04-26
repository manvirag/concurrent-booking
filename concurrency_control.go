package main

import (
	"context"
	"database/sql"

	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

func generatePlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.Repeat("?,", count-1) + "?"
}

func sliceToInterface(ids []int) []interface{} {
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return args
}

// PessimisticLocking: First come, first serve approach for seat booking
func PessimisticLocking(ctx context.Context, db *sql.DB, userID int, seatIDs []int, bookingId string) error {
	log.Printf("[Booking] Starting pessimistic locking - UserID: %d, Seats: %v", userID, seatIDs)

	if len(seatIDs) == 0 {
		log.Printf("[Booking] No seat IDs provided - UserID: %d", userID)
		return fmt.Errorf("no seat IDs provided")
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		log.Printf("[Booking] Failed to begin transaction - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// 1. Lock Seats
	placeholders := generatePlaceholders(len(seatIDs))
	lockQuery := fmt.Sprintf("SELECT id FROM seats WHERE id IN (%s) AND (is_reserved = 0 OR (is_reserved = 1 AND payment_status = 'FAILED')) FOR UPDATE", placeholders)
	lockArgs := sliceToInterface(seatIDs)

	log.Printf("[Booking] Attempting to lock seats - UserID: %d, Query: %s, Args: %v", userID, lockQuery, lockArgs)
	rows, err := tx.QueryContext(ctx, lockQuery, lockArgs...)
	if err != nil {
		log.Printf("[Booking] Failed to query seats for locking - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to query seats for locking: %w", err)
	}
	defer rows.Close()

	lockedSeatsCount := 0
	for rows.Next() {
		lockedSeatsCount++
	}
	if err = rows.Err(); err != nil {
		log.Printf("[Booking] Error iterating locked seat rows - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("error iterating locked seat rows: %w", err)
	}

	if lockedSeatsCount != len(seatIDs) {
		log.Printf("[Booking] Not all seats available - UserID: %d, Requested: %d, Available: %d",
			userID, len(seatIDs), lockedSeatsCount)
		return fmt.Errorf("all seats are not available for booking")
	}

	sessionID := bookingId
	redirectURL := fmt.Sprintf("https://payment-gateway.example.com/pay/%s", sessionID)
	log.Printf("[Booking] Generated payment session - UserID: %d, SessionID: %s", userID, sessionID)

	// 2. Update Seats
	updatePlaceholders := generatePlaceholders(len(seatIDs))
	updateQuery := fmt.Sprintf(`
		UPDATE seats 
		SET is_reserved = 1, 
		    payment_status = 'PENDING',
			user_id = ?, 
			payment_session_id = ?,
            payment_redirect_url = ?,
            payment_timeout = ?
		WHERE id IN (%s)`, updatePlaceholders)

	updateArgs := make([]interface{}, 0, len(seatIDs)+4)
	updateArgs = append(updateArgs, userID)
	updateArgs = append(updateArgs, sessionID)
	updateArgs = append(updateArgs, redirectURL)
	updateArgs = append(updateArgs, time.Now().Add(time.Minute))
	updateArgs = append(updateArgs, sliceToInterface(seatIDs)...)

	log.Printf("[Booking] Updating seats - UserID: %d, SessionID: %s", userID, sessionID)
	_, err = tx.ExecContext(ctx, updateQuery, updateArgs...)
	if err != nil {
		log.Printf("[Booking] Failed to mark seats as reserved - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to mark seats as reserved: %w", err)
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[Booking] Failed to commit transaction - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("[Booking] Successfully completed pessimistic locking - UserID: %d, SessionID: %s", userID, sessionID)
	return nil
}

// OptimisticLocking: Let multiple users try to book, but only first successful payment wins
func OptimisticLocking(ctx context.Context, db *sql.DB, userID int, seatIDs []int, bookingId string) error {
	log.Printf("[Booking] Starting optimistic locking - UserID: %d, Seats: %v", userID, seatIDs)

	if len(seatIDs) == 0 {
		log.Printf("[Booking] No seat IDs provided - UserID: %d", userID)
		return fmt.Errorf("no seat IDs provided")
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		log.Printf("[Booking] Failed to begin transaction - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	placeholders := generatePlaceholders(len(seatIDs))
	selectQuery := fmt.Sprintf(`
		SELECT id, version 
		FROM seats 
		WHERE id IN (%s) 
		AND (is_reserved = 0 OR (is_reserved = 1 AND payment_status = 'FAILED'))`, placeholders)
	selectArgs := sliceToInterface(seatIDs)

	log.Printf("[Booking] Checking seat versions - UserID: %d, Query: %s", userID, selectQuery)
	rows, err := tx.QueryContext(ctx, selectQuery, selectArgs...)
	if err != nil {
		log.Printf("[Booking] Failed to get seat versions - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to get seat versions: %w", err)
	}
	defer rows.Close()

	seatVersions := make(map[int]int)
	countFound := 0
	for rows.Next() {
		var seatID, version int
		if err := rows.Scan(&seatID, &version); err != nil {
			log.Printf("[Booking] Failed to scan seat version - UserID: %d, Error: %v", userID, err)
			return fmt.Errorf("failed to scan seat version: %v", err)
		}
		seatVersions[seatID] = version
		countFound++
	}
	if err = rows.Err(); err != nil {
		log.Printf("[Booking] Error iterating seat version rows - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("error iterating seat version rows: %w", err)
	}

	if countFound != len(seatIDs) {
		log.Printf("[Booking] Not all seats available - UserID: %d, Requested: %d, Found: %d",
			userID, len(seatIDs), countFound)
		return fmt.Errorf("seats are not available or have pending/successful payment")
	}

	sessionID := bookingId
	redirectURL := fmt.Sprintf("https://payment-gateway.example.com/pay/%s", sessionID)
	log.Printf("[Booking] Generated payment session - UserID: %d, SessionID: %s", userID, sessionID)

	updateQuery := `	
		UPDATE seats 
		SET is_reserved = 1, 
			user_id = ?, 
			payment_status = 'PENDING',
			payment_session_id = ?,
            payment_redirect_url = ?,
            payment_timeout = ?,
			version = version + 1
		WHERE id = ? 
		AND version = ? 
        AND (is_reserved = 0 OR (is_reserved = 1 AND payment_status = 'FAILED')) 
	`
	updateArgs := make([]interface{}, 0, 6)
	updateArgs = append(updateArgs, userID)
	updateArgs = append(updateArgs, sessionID)
	updateArgs = append(updateArgs, redirectURL)
	updateArgs = append(updateArgs, time.Now().Add(time.Minute))

	var updatedSeatIDs []int
	for _, seatID := range seatIDs {
		version := seatVersions[seatID]
		seatUpdateArgs := append(updateArgs, seatID, version)

		log.Printf("[Booking] Updating seat - UserID: %d, SeatID: %d, Version: %d", userID, seatID, version)
		result, err := tx.ExecContext(ctx, updateQuery, seatUpdateArgs...)
		if err != nil {
			log.Printf("[Booking] Failed to update seat - UserID: %d, SeatID: %d, Error: %v", userID, seatID, err)
			return fmt.Errorf("failed to update seat %d: %w", seatID, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			log.Printf("[Booking] Failed to get rows affected - UserID: %d, SeatID: %d, Error: %v", userID, seatID, err)
			return fmt.Errorf("failed to get rows affected for seat %d: %w", seatID, err)
		}

		if rowsAffected == 0 {
			log.Printf("[Booking] Optimistic lock conflict - UserID: %d, SeatID: %d", userID, seatID)
			return fmt.Errorf("optimistic lock conflict on seat %d", seatID)
		}
		updatedSeatIDs = append(updatedSeatIDs, seatID)
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[Booking] Failed to commit transaction - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("[Booking] Successfully completed optimistic locking - UserID: %d, SessionID: %s", userID, sessionID)
	return nil
}

// CurrentImplementation: Simple approach using Redis locks first, then database transaction
func BookMyShowTimeoutImp(ctx context.Context, db *sql.DB, redisClient *redis.Client, userID int, seatIDs []int, bookingId string) error {
	log.Printf("[Booking] Starting timeout-based booking - UserID: %d, Seats: %v", userID, seatIDs)

	if len(seatIDs) == 0 {
		log.Printf("[Booking] No seat IDs provided - UserID: %d", userID)
		return fmt.Errorf("no seat IDs provided")
	}

	lockKey := fmt.Sprintf("seat_lock:%d", seatIDs[0])
	lockValue := fmt.Sprintf("user:%d", userID)
	lockTimeout := 1 * time.Minute

	log.Printf("[Booking] Attempting to acquire Redis lock - UserID: %d, LockKey: %s", userID, lockKey)
	locked, err := redisClient.SetNX(ctx, lockKey, lockValue, lockTimeout).Result()
	if err != nil {
		log.Printf("[Booking] Redis error while acquiring lock - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to check/set Redis lock for key %s: %w", lockKey, err)
	}
	if !locked {
		holder, _ := redisClient.Get(ctx, lockKey).Result()
		log.Printf("[Booking] Failed to acquire Redis lock - UserID: %d, Current Holder: %s", userID, holder)
		return fmt.Errorf("failed to acquire Redis lock for seats (key: %s), possibly locked by another user", lockKey)
	}

	log.Printf("[Booking] Acquired Redis lock - UserID: %d, LockKey: %s", userID, lockKey)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		log.Printf("[Booking] Failed to begin transaction - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	placeholders := generatePlaceholders(len(seatIDs))
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM seats WHERE id IN (%s) AND (is_reserved = 0 OR (is_reserved = 1 AND payment_status = 'FAILED')) FOR UPDATE", placeholders)
	checkArgs := sliceToInterface(seatIDs)

	log.Printf("[Booking] Checking seat availability - UserID: %d", userID)
	var availableCount int
	err = tx.QueryRowContext(ctx, checkQuery, checkArgs...).Scan(&availableCount)
	if err != nil {
		log.Printf("[Booking] Failed to check seat availability - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to check seat availability in DB: %w", err)
	}

	if availableCount != len(seatIDs) {
		log.Printf("[Booking] Not all seats available - UserID: %d, Requested: %d, Available: %d",
			userID, len(seatIDs), availableCount)
		return fmt.Errorf("not all seats are available in DB despite acquiring lock (%d/%d available)", availableCount, len(seatIDs))
	}

	sessionID := bookingId
	redirectURL := fmt.Sprintf("https://payment-gateway.example.com/pay/%s", sessionID)
	log.Printf("[Booking] Generated payment session - UserID: %d, SessionID: %s", userID, sessionID)

	updateQuery := fmt.Sprintf(`
		UPDATE seats 
		SET is_reserved = 1, 
		    payment_status = 'PENDING',
			user_id = ?, 
			payment_session_id = ?,
            payment_redirect_url = ?,
            payment_timeout = ?
		WHERE id IN (%s)`, placeholders)

	updateArgs := make([]interface{}, 0, len(seatIDs)+4)
	updateArgs = append(updateArgs, userID)
	updateArgs = append(updateArgs, sessionID)
	updateArgs = append(updateArgs, redirectURL)
	updateArgs = append(updateArgs, time.Now().Add(time.Minute))
	updateArgs = append(updateArgs, sliceToInterface(seatIDs)...)

	log.Printf("[Booking] Updating seats - UserID: %d, SessionID: %s", userID, sessionID)
	_, err = tx.ExecContext(ctx, updateQuery, updateArgs...)
	if err != nil {
		log.Printf("[Booking] Failed to mark seats as reserved - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to mark seats as reserved in DB: %w", err)
	}

	if err := tx.Commit(); err != nil {
		log.Printf("[Booking] Failed to commit transaction - UserID: %d, Error: %v", userID, err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("[Booking] Successfully completed timeout-based booking - UserID: %d, SessionID: %s", userID, sessionID)
	return nil
}
