package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Seat struct {
	ID     int
	ShowID int
}

type BookingRequest struct {
	UserID  int
	ShowID  int
	SeatIDs []int
	Method  string // "pessimistic", "optimistic", or "current"
}

type AsyncBookingResponse struct {
	BookingID string `json:"booking_id"`
	Status    string `json:"status"`
}

var (
	db  *sql.DB
	rdb *redis.Client
	ctx = context.Background()
)

func BookSeats(req BookingRequest, bookingId string) error {
	var err error

	// Choose concurrency control method based on request
	switch req.Method {
	case "pessimistic":
		err = PessimisticLocking(ctx, db, req.UserID, req.SeatIDs, bookingId)
	case "optimistic":
		err = OptimisticLocking(ctx, db, req.UserID, req.SeatIDs, bookingId)
	case "current":
		err = BookMyShowTimeoutImp(ctx, db, rdb, req.UserID, req.SeatIDs, bookingId)
	default:
		return fmt.Errorf("invalid concurrency control method: %s", req.Method)
	}

	if err != nil {
		return err
	}
	return nil

}

func handlePaymentWebhook(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Webhook] Payment webhook received from IP: %s", r.RemoteAddr)

	if r.Method != http.MethodPost {
		log.Printf("[Webhook] Invalid method %s from IP: %s", r.Method, r.RemoteAddr)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		SessionID string `json:"session_id"`
		Status    string `json:"status"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		log.Printf("[Webhook] Invalid payload from IP: %s, Error: %v", r.RemoteAddr, err)
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	log.Printf("[Webhook] Processing payment - SessionID: %s, Status: %s", payload.SessionID, payload.Status)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})

	if err != nil {
		fmt.Printf("Failed at transaction beginning. %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	defer tx.Rollback()

	fmt.Printf("select pending rows %v", payload)

	query := `
	SELECT id, user_id, version FROM seats 
	WHERE payment_session_id = ? AND payment_status = 'PENDING'
`

	rows, err := tx.QueryContext(ctx, query, payload.SessionID)
	if err != nil {
		fmt.Printf("failed at fetching pending data %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var seatVersions = make(map[int]int)
	var seatUser = make(map[int]int)
	for rows.Next() {
		fmt.Println(rows)
		var seatID, version, user_id int
		if err := rows.Scan(&seatID, &user_id, &version); err != nil {
			fmt.Printf("failed at scaning data %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		seatVersions[seatID] = version
		seatUser[seatID] = user_id
	}

	fmt.Println(seatUser)
	fmt.Println(seatVersions)

	if len(seatVersions) == 0 {
		http.Error(w, "No pending seats found", http.StatusNotFound)
		return
	}

	for seatID, version := range seatVersions {
		result, err := tx.ExecContext(ctx, `
            UPDATE seats 
            SET payment_status = ?,
                version = version + 1
            WHERE id = ? AND version = ?
        `, payload.Status, seatID, version)
		if err != nil {
			fmt.Printf("failed at updating the seats %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			fmt.Printf("failed at result.RowsAffected() %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		if rowsAffected == 0 {
			fmt.Printf("Concurrent modification detected %v", err)
			http.Error(w, "Concurrent modification detected", http.StatusConflict)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		fmt.Printf("failing at commit %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Cleanup Redis Lock
	for seatID, userId := range seatUser {
		lockKey := fmt.Sprintf("seat_lock:%d", seatID)
		lockValue := fmt.Sprintf("user:%d", userId)

		val, err := rdb.Get(ctx, lockKey).Result()
		if err == nil && val == lockValue {
			rdb.Del(ctx, lockKey)
			log.Printf("[Webhook] Released Redis lock - SeatID: %d, UserID: %d, LockKey: %s",
				seatID, userId, lockKey)
		}
	}

	log.Printf("[Webhook] Successfully processed payment - SessionID: %s, Status: %s",
		payload.SessionID, payload.Status)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

func handleAsyncBooking(w http.ResponseWriter, r *http.Request) {
	log.Printf("[API] Starting async booking request from IP: %s", r.RemoteAddr)

	if r.Method != http.MethodPost {
		log.Printf("[API] Invalid method %s from IP: %s", r.Method, r.RemoteAddr)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req BookingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[API] Invalid request body from IP: %s, error: %v", r.RemoteAddr, err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("[API] Valid booking request - UserID: %d, ShowID: %d, Seats: %v, Method: %s",
		req.UserID, req.ShowID, req.SeatIDs, req.Method)

	bookingID := fmt.Sprintf("book_%d_%d", req.UserID, time.Now().UnixNano())
	log.Printf("[API] Generated booking ID: %s for UserID: %d", bookingID, req.UserID)

	log.Printf("[Booking] Starting booking process - BookingID: %s, UserID: %d", bookingID, req.UserID)

	err := BookSeats(req, bookingID)
	if err != nil {
		log.Printf("[Booking] Failed booking - BookingID: %s, UserID: %d, Error: %v",
			bookingID, req.UserID, err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(AsyncBookingResponse{
			BookingID: bookingID,
			Status:    "FAILED",
		})
	} else {
		log.Printf("[Booking] Successfully initiated booking - BookingID: %s, UserID: %d",
			bookingID, req.UserID)

		log.Printf("[API] Returning booking response - BookingID: %s, Status: PENDING", bookingID)
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(AsyncBookingResponse{
			BookingID: bookingID,
			Status:    "PENDING",
		})
	}

}

func handleBookingStatus(w http.ResponseWriter, r *http.Request) {
	log.Printf("[API] Status check request from IP: %s", r.RemoteAddr)

	if r.Method != http.MethodGet {
		log.Printf("[API] Invalid method %s from IP: %s", r.Method, r.RemoteAddr)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bookingID := r.URL.Query().Get("booking_id")
	if bookingID == "" {
		log.Printf("[API] Missing booking_id parameter from IP: %s", r.RemoteAddr)
		http.Error(w, "Booking ID is required", http.StatusBadRequest)
		return
	}

	log.Printf("[API] Checking status for BookingID: %s", bookingID)

	var status string
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(MIN(payment_status), 'NOT_FOUND') as status
		FROM seats 
		WHERE payment_session_id = ?
	`, bookingID).Scan(&status)

	if err != nil {
		log.Printf("[API] Database error while checking status - BookingID: %s, Error: %v", bookingID, err)
		http.Error(w, "Error fetching booking status", http.StatusInternalServerError)
		return
	}

	if status == "NOT_FOUND" {
		log.Printf("[API] Booking not found - BookingID: %s", bookingID)
		http.Error(w, "Booking not found", http.StatusNotFound)
		return
	}

	log.Printf("[API] Retrieved status for BookingID: %s - Status: %s", bookingID, status)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(AsyncBookingResponse{
		BookingID: bookingID,
		Status:    status,
	})
}

func startServer() error {
	http.HandleFunc("/webhook/payment", handlePaymentWebhook)
	http.HandleFunc("/api/book", handleAsyncBooking)
	http.HandleFunc("/api/booking-status", handleBookingStatus)
	log.Fatal(http.ListenAndServe(":8081", nil))
	return errors.New("ending server")
}

func checkPaymentTimeouts() error {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			log.Printf("Error starting transaction: %v", err)
			continue
		}

		rows, err := tx.QueryContext(ctx, `
            SELECT id, show_id, user_id 
            FROM seats 
            WHERE payment_status = 'PENDING' 
            AND payment_timeout < NOW()
        `)
		if err != nil {
			tx.Rollback()
			log.Printf("Error querying expired payments: %v", err)
			continue
		}

		var expiredSeats []struct {
			id     int
			showID int
			userID int
		}

		for rows.Next() {
			var seat struct {
				id     int
				showID int
				userID int
			}
			if err := rows.Scan(&seat.id, &seat.showID, &seat.userID); err != nil {
				log.Printf("Error scanning seat: %v", err)
				continue
			}
			expiredSeats = append(expiredSeats, seat)
		}
		rows.Close()

		for _, seat := range expiredSeats {
			_, err := tx.ExecContext(ctx, `
                UPDATE seats 
                SET is_reserved = FALSE,
                    payment_status = 'FAILED',
                    user_id = NULL,
                    reserved_until = NULL,
                    payment_timeout = NULL,
                    payment_session_id = NULL,
                    payment_redirect_url = NULL
                WHERE id = ?
            `, seat.id)
			if err != nil {
				log.Printf("Error updating expired seat %d: %v", seat.id, err)
				continue
			}

			key := fmt.Sprintf("lock:seat:%d", seat.id)
			rdb.Del(ctx, key)
		}

		if err := tx.Commit(); err != nil {
			log.Printf("Error committing transaction: %v", err)
		}
	}

	return errors.New("ending timeout payment function")
}

func main() {
	var err error
	db, err = sql.Open("mysql", "root:password@tcp(localhost:3306)/bms")
	if err != nil {
		log.Fatal(err)
	}

	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}

	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// Test Redis connection
	if err = rdb.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}

	errorCh := make(chan error, 2)
	go func() {
		err := checkPaymentTimeouts()
		errorCh <- err
	}()

	go func() {
		err := startServer()
		errorCh <- err
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	select {
	case gErr := <-errorCh:
		log.Fatalf("Service error: %v", gErr)
	case sig := <-sigs:
		log.Printf("Received signal: %v, shutting down gracefully", sig)
	}
}
