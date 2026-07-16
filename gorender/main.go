package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Job
type Job struct {
	Id             int
	Name           string
	BuildDuration  time.Duration
	currentAttempt int
	failedAttempts int
}

// jobstatus
type JobStatus string

const (
	statusPending   JobStatus = "PENDING"
	statusRunning   JobStatus = "RUNNING"
	statusCompleted JobStatus = "COMPLETED"
	statusFailed    JobStatus = "FAILED"
)

// jobstore used to store the status -> db
type JobStore struct {
	mu       sync.RWMutex
	statuses map[int]JobStatus ``
}

// methods for setting status
func (s *JobStore) SetStatus(jobId int, jobStatus JobStatus) {
	// exclusive lock (write only)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[jobId] = jobStatus
	// saving the logs to file
	s.SaveToFile()
}

// mehtod for getting status
func (s *JobStore) GetStatus(jobId int) JobStatus {
	//    shared lock (multiple can read but block when write)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if value, ok := s.statuses[jobId]; ok {
		return value
	}
	return ""
}

// method for writing to file
func (s *JobStore) SaveToFile() error {
	bytes, jsonErr := json.Marshal(s.statuses)
	if jsonErr != nil {
		fmt.Print("Error: failed to convert json:", jsonErr)
		return jsonErr
	}

	err := os.WriteFile("jobs.json", bytes, 0644)
	if err != nil {
		fmt.Print("Error: failed to persist status to disk:", err)
		return err
	}
	return nil
}

type Metrics struct {
	mu            sync.RWMutex
	JobsSubmitted int
	JobsCompleted int
	JobsFailed    int
	JobsActive    int
}

func (m *Metrics) SetJobsSubmitted(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsSubmitted += n
}

func (m *Metrics) SetJobsCompleted(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsCompleted += n
}

func (m *Metrics) SetJobsFailed(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsFailed += n
}

func (m *Metrics) SetJobActive(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsActive += n
}

func (m *Metrics) GetMetrics() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fmt.Printf("Total Jobs:%d \n Completed:%d \n Failed:%d \n Active:%d", m.JobsSubmitted, m.JobsCompleted, m.JobsFailed, m.JobsActive)
}

// creating a jobqueue with 100 predefine size
var JobQeue = make(chan Job, 100)

// constructor function for intitalizing jobsStore
func NewJobStore() *JobStore {
	return &JobStore{
		statuses: make(map[int]JobStatus),
	}
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

const MaxFailedRetry = 3

func main() {
	// initializing the empty store
	NewJobStore := NewJobStore()
	NewMetrics := NewMetrics()

	// handling gracefull shutdown
	// registering the interupptions
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// WaitGroups
	var jobWg sync.WaitGroup
	var workerWg sync.WaitGroup

	// pool size
	const workerPoolSize = 3

	// spawning the worker
	for range workerPoolSize {
		workerWg.Add(1)
		go JobWorker(&jobWg, &workerWg, NewJobStore, ctx, NewMetrics)
	}

	// pushing jobs to queue
	JobSubmitter(&jobWg, Job{Id: 1, Name: "Test", BuildDuration: time.Second, currentAttempt: 0, failedAttempts: 0}, NewJobStore, NewMetrics)
	JobSubmitter(&jobWg, Job{Id: 2, Name: "Test", BuildDuration: time.Second, currentAttempt: 0, failedAttempts: 1}, NewJobStore, NewMetrics)
	JobSubmitter(&jobWg, Job{Id: 3, Name: "Test", BuildDuration: time.Second, currentAttempt: 0, failedAttempts: 2}, NewJobStore, NewMetrics)
	JobSubmitter(&jobWg, Job{Id: 4, Name: "Test", BuildDuration: time.Second, currentAttempt: 0, failedAttempts: 4}, NewJobStore, NewMetrics)

	// wating for goroutines to finish executions
	jobsDone := make(chan struct{})

	// monitor thread
	go func() {
		jobWg.Wait()
		close(jobsDone)
	}()

	select {
	case <-jobsDone:
		fmt.Println("All Jobs processed")
	case <-ctx.Done():
		fmt.Print("Interuption has occured")
	}
	//
	close(JobQeue)
	workerWg.Wait()
	// closing the queue
	NewMetrics.GetMetrics()
}
func JobSubmitter(jobWg *sync.WaitGroup, job Job, s *JobStore, m *Metrics) {
	m.SetJobsSubmitted(1)
	jobWg.Add(1)
	s.SetStatus(job.Id, statusPending)
	fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
	JobQeue <- job
}
func JobWorker(jobWg *sync.WaitGroup, workerWg *sync.WaitGroup, s *JobStore, ctx context.Context, m *Metrics) {

	defer workerWg.Done()
	for {
		select {
		case <-ctx.Done():
			// stop picking new job
			return
		case job, ok := <-JobQeue:
			if !ok {
				return
			}
			s.SetStatus(job.Id, statusRunning)
			m.SetJobActive(1)
			err := BuildJob(job)
			job.currentAttempt += 1
			if err != nil {
				s.SetStatus(job.Id, statusFailed)
				fmt.Println(err)
				if job.currentAttempt < MaxFailedRetry {
					// Retry Logic (spawning a new goroutine )
					delay := time.Second * time.Duration(
						1<<(job.currentAttempt-1))
					RetryFailedJob(s, job, delay, ctx)
					m.SetJobActive(-1)
				} else {
					fmt.Println("id:", job.Id, "failed")
					// permanently failed
					m.SetJobsFailed(1)
					m.SetJobActive(-1)
					jobWg.Done()
				}
				continue
			}
			fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
			time.Sleep(job.BuildDuration)
			s.SetStatus(job.Id, statusCompleted)
			m.SetJobsCompleted(1)
			m.SetJobActive(-1)
			fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
			jobWg.Done()
		}

	}
}
func BuildJob(job Job) error {
	if job.currentAttempt < job.failedAttempts {
		return fmt.Errorf("id:%d simulated build failure:%d (attempt %d)", job.Id, job.failedAttempts, job.currentAttempt+1)
	}
	return nil
}

func RetryFailedJob(s *JobStore, job Job, delay time.Duration, ctx context.Context) {

	go func() {
		select {
		case <-time.After(delay):
			select {
			case <-ctx.Done():
				return
			default:
				// delayed retry
				s.SetStatus(job.Id, statusPending)
				fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
				JobQeue <- job

			}
		}

	}()

}
