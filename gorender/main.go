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

var JobQeue = make(chan Job, 100)

func main() {
	var wg sync.WaitGroup
	const workerPoolSize = 3

	for range workerPoolSize {
		wg.Add(1)
		go JobWorker(&wg)
	}

	for i := 0; i < 3; i++ {
		JobSubmitter(Job{Id: i, Name: "Test1", BuildDuration: time.Second})

	}

	close(JobQeue)

	// wating for goroutines to finish executions
	wg.Wait()
}

func JobSubmitter(job Job) {
	JobQeue <- job
	fmt.Println("jobId:", job.Id, "pushes succesfully")
}

func JobWorker(wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range JobQeue {
		time.Sleep(job.BuildDuration)
		fmt.Println("id:", job.Id, job.Name, "succesfully build")
	}

}
