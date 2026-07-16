package main

import (
	"context"
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
		go JobWorker(&jobWg, &workerWg, NewJobStore, ctx)
	}

	// pushing jobs to queue
	JobSubmitter(&jobWg, Job{Id: 1, Name: "Test", BuildDuration: time.Second, currentAttempt: 0, failedAttempts: 0}, NewJobStore)
	JobSubmitter(&jobWg, Job{Id: 2, Name: "Test", BuildDuration: time.Second, currentAttempt: 0, failedAttempts: 1}, NewJobStore)
	JobSubmitter(&jobWg, Job{Id: 3, Name: "Test", BuildDuration: time.Second, currentAttempt: 0, failedAttempts: 2}, NewJobStore)
	JobSubmitter(&jobWg, Job{Id: 4, Name: "Test", BuildDuration: time.Second, currentAttempt: 0, failedAttempts: 4}, NewJobStore)

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
}
func JobSubmitter(jobWg *sync.WaitGroup, job Job, s *JobStore) {
	jobWg.Add(1)
	s.SetStatus(job.Id, statusPending)
	fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
	JobQeue <- job
}
func JobWorker(jobWg *sync.WaitGroup, workerWg *sync.WaitGroup, s *JobStore, ctx context.Context) {

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
			err := BuildJob(job)
			job.currentAttempt += 1
			if err != nil {
				s.SetStatus(job.Id, statusFailed)
				fmt.Println(err)
				if job.currentAttempt < MaxFailedRetry {
					// Retry Logic (spawning a new goroutine )
					delay := time.Second * time.Duration(
						1<<(job.currentAttempt-1))
					RetryFailedJob(s, job, delay)
				} else {
					fmt.Println("id:", job.Id, "failed")
					// permanently failed
					jobWg.Done()
				}
				continue
			}
			fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
			time.Sleep(job.BuildDuration)
			s.SetStatus(job.Id, statusCompleted)
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

func RetryFailedJob(s *JobStore, job Job, delay time.Duration) {
	go func() {
		time.Sleep(delay)
		s.SetStatus(job.Id, statusPending)
		fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
		JobQeue <- job
	}()

}
