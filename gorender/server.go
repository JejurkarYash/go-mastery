package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type RequestDataType struct {
	Name          string `json:"name"`
	BuildDuration int    `json:"buildDuration"`
}

var globalId int64

func StartServer(ctx context.Context, jobWg *sync.WaitGroup, store *JobStore, metrics *Metrics) {

	// http handler(router)
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Server is running on Port:8080"))
	})

	// handling the deploy function
	mux.HandleFunc("POST /deploy", func(w http.ResponseWriter, r *http.Request) {
		requestData, err := ReadJSON(r)
		if err != nil {
			fmt.Println("Error: Reading json", err)
		}
		nextId := atomic.AddInt64(&globalId, 1)
		job := Job{Id: int(nextId), Name: requestData.Name, BuildDuration: time.Duration(requestData.BuildDuration)}
		JobSubmitter(jobWg, job, store, metrics)
		WriteJSON(w, http.StatusOK, "Job Submitted")
	})

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics.mu.Lock()
		defer metrics.mu.Unlock()

		WriteJSON(w, http.StatusOK, metrics)
	})

	// instantating server
	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// start a server in background
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP sercer ListenAndServer error:%v\n", err)
		}
	}()
	fmt.Println("Server is running...")

	// spawning a thread to monitor for this only
	go func() {
		<-ctx.Done()
		fmt.Println("shutting down the server")

		shutDownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutDownCtx); err != nil {
			fmt.Println("error:shutting down server")
		}
		fmt.Print("server stopped")
	}()
}

// generic function to read json
func ReadJSON(r *http.Request) (*RequestDataType, error) {
	requestData := RequestDataType{}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&requestData); err != nil {
		fmt.Println("Error:decoding the reqeest", err)
		return nil, err
	}
	return &requestData, nil
}

// generic function for writing error
func WriteError(w http.ResponseWriter, status int, m string) {
	type ErrorResponse struct {
		Error string `json:"error"`
	}
	WriteJSON(w, status, ErrorResponse{Error: m})
}

// generic function to write responseH
func WriteJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "applications/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(v)
}
