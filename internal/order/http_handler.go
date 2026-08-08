package order

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/example/realtime-data-platform/internal/events"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Handler adapts HTTP requests to the order application service.
type Handler struct {
	service *Service
	db      *pgxpool.Pool
	events  *events.Store
	logger  *slog.Logger
}

// NewHandler creates the HTTP adapter.
func NewHandler(service *Service, db *pgxpool.Pool, eventStore *events.Store, logger *slog.Logger) *Handler {
	return &Handler{service: service, db: db, events: eventStore, logger: logger}
}

// Routes returns the public HTTP handler for the service.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /orders", h.create)
	mux.HandleFunc("GET /events/latest", h.latestEvent)
	return mux
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if err := h.db.Ping(r.Context()); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) latestEvent(w http.ResponseWriter, r *http.Request) {
	data, ok := h.events.Get()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"message": "no order event observed yet"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var request CreateOrderRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	// Reject a second JSON value after the request object, such as {} {}.
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		http.Error(w, "request body must contain one JSON object", http.StatusBadRequest)
		return
	}

	response, err := h.service.Create(r.Context(), request)
	if err != nil {
		var validationErr *ValidationError
		if errors.As(err, &validationErr) {
			http.Error(w, validationErr.Error(), http.StatusBadRequest)
			return
		}
		h.logger.Error("create order", "error", err)
		http.Error(w, "failed to create order", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, response)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
