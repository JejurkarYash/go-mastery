package main

import (
	"fmt"
	"sync"
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
	statuses map[int]JobStatus
}

// methods for setting status
func (s *JobStore) SetStatus(jobId int, jobStatus JobStatus) {
	// exclusive lock (write only)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[jobId] = jobStatus
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

// creating a jobqueue with 100 predefine size
var JobQeue = make(chan Job, 100)

// constructor function
func NewJobStore() *JobStore {
	return &JobStore{
		statuses: make(map[int]JobStatus),
	}
}

const MaxFailedRetry = 3

func main() {
	// initializing the empty store
	NewJobStore := NewJobStore()

	var wg sync.WaitGroup
	const workerPoolSize = 3

	for range workerPoolSize {
		go JobWorker(&wg, NewJobStore)
	}
	// pushing the job into jobqueue
	for i := 0; i < 3; i++ {
		JobSubmitter(&wg, Job{Id: i, Name: "Test", BuildDuration: time.Second, currentAttempt: 0, failedAttempts: i}, NewJobStore)
	}

	// wating for goroutines to finish executions
	wg.Wait()
	// closing the queue
	close(JobQeue)
}

func JobSubmitter(wg *sync.WaitGroup, job Job, s *JobStore) {
	wg.Add(1)
	s.SetStatus(job.Id, statusPending)
	fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
	JobQeue <- job
}

func JobWorker(wg *sync.WaitGroup, s *JobStore) {
	for job := range JobQeue {
		s.SetStatus(job.Id, statusRunning)
		err := BuildJob(job)
		job.currentAttempt += 1
		if err != nil {
			s.SetStatus(job.Id, statusFailed)
			fmt.Println(err)
			if job.currentAttempt < MaxFailedRetry {
				// Retry Logic
				RetryFailedJob(s, job)
			} else {
				fmt.Println("id:", job.Id, "failed")
				// permanently failed
				wg.Done()
			}
			continue
		}
		fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
		time.Sleep(job.BuildDuration)
		s.SetStatus(job.Id, statusCompleted)
		fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
		wg.Done()

	}
}

func BuildJob(job Job) error {
	if job.currentAttempt < job.failedAttempts {
		return fmt.Errorf("id:%d simulated build failure:%d (attempt %d)", job.Id, job.failedAttempts, job.currentAttempt+1)
	}
	return nil
}

func RetryFailedJob(s *JobStore, job Job) {
	s.SetStatus(job.Id, statusPending)
	fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
	JobQeue <- job
}
