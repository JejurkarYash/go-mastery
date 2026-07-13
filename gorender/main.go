package main

import (
	"fmt"
	"sync"
	"time"
)

type Job struct {
	Id            int
	Name          string
	BuildDuration time.Duration
}

type JobStatus string

const (
	statusPending   JobStatus = "PENDING"
	statusRunning   JobStatus = "RUNNING"
	statusCompleted JobStatus = "COMPLETED"
	statusFailed    JobStatus = "FAILED"
)

type JobStore struct {
	mu       sync.RWMutex
	statuses map[int]JobStatus
}

// methods for setting status
func (s *JobStore) SetStatus(jobId int, jobStatus JobStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[jobId] = jobStatus
}

// mehtod for getting status
func (s *JobStore) GetStatus(jobId int) JobStatus {

	s.mu.RLock()
	defer s.mu.RUnlock()

	if value, ok := s.statuses[jobId]; ok {
		return value
	}
	return ""
}

var JobQeue = make(chan Job, 100)

// constructor function
func NewJobStore() *JobStore {
	return &JobStore{
		statuses: make(map[int]JobStatus),
	}
}
func main() {
	// empty store
	NewJobStore := NewJobStore()

	var wg sync.WaitGroup
	const workerPoolSize = 3

	for range workerPoolSize {
		wg.Add(1)
		go JobWorker(&wg, NewJobStore)
	}

	for i := 0; i < 3; i++ {
		JobSubmitter(Job{Id: i, Name: "Test1", BuildDuration: time.Second}, NewJobStore)

	}

	close(JobQeue)

	// wating for goroutines to finish executions
	wg.Wait()
}

func JobSubmitter(job Job, s *JobStore) {
	s.SetStatus(job.Id, statusPending)
	fmt.Println("id:", job.Id, "status:", s.GetStatus(job.Id))
	JobQeue <- job
	fmt.Println("jobId:", job.Id, "pushes succesfully")
}

func JobWorker(wg *sync.WaitGroup, s *JobStore) {
	defer wg.Done()
	for job := range JobQeue {
		s.SetStatus(job.Id, statusRunning)
		fmt.Println("id:", job.Id, s.GetStatus(job.Id))
		time.Sleep(job.BuildDuration)
		fmt.Println("id:", job.Id, job.Name, "succesfully build")
		s.SetStatus(job.Id, statusCompleted)
		fmt.Println("id:", job.Id, s.GetStatus(job.Id))
	}

}
