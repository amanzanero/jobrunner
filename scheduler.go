package jobrunner

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

/*
The scheduler is responsible for querying jobs to execute and sending them to the job queue
*/

type Job interface {
	Execute(ctx context.Context) error
}

type JobReader interface {
	Read(t time.Time, jobs []Job) (int, error)
}

type JobProducer struct {
	Reader   JobReader
	JobQueue chan<- Job
	quit     chan bool
	finished chan bool
}

func (p *JobProducer) pollAndQueueJobs(jobs []Job) {
	// poll for jobs
	numJobs, err := p.Reader.Read(time.Now(), jobs)
	if err != nil {
		log.Println("failed to read jobs, will try again in a minute")
		return
	}
	log.Println("JobProducer", fmt.Sprintf("enqueing %d jobs", numJobs))
	for i := 0; i < numJobs; i++ {
		p.JobQueue <- jobs[i]
	}
}

func (p *JobProducer) Start() {
	log.Println("JobProducer starting...")

	p.quit = make(chan bool)
	p.finished = make(chan bool)

	go func() {
		// 1) poll for jobs immediately in case there are jobs that need to be run
		jobs := make([]Job, 100)
		p.pollAndQueueJobs(jobs)

		// 2) then wait until an even minute
		oneMin := int64(time.Minute)
		duration := time.Duration(oneMin - (time.Now().UnixNano() % oneMin))
		log.Println(fmt.Sprintf("sleeping for %f seconds", duration.Seconds()))
		select {
		case <-p.quit:
			log.Println("JobProducer received quit signal")
			p.finished <- true
			return
		case <-time.After(duration):
		}
		ticker := time.NewTicker(time.Minute)

		// 3) queue up jobs at even minute
		p.pollAndQueueJobs(jobs)

		// 4) run loop to load jobs
		for {
			select {
			case ct := <-ticker.C:
				log.Println(fmt.Sprintf("jop producer waking to poll for jobs (%v)", ct))
				// poll for jobs
				p.pollAndQueueJobs(jobs)
			case <-p.quit:
				log.Println("JobProducer received quit signal")
				p.finished <- true
			}
		}
	}()
}

func (p *JobProducer) Stop(ctx context.Context) {
	log.Println("JobProducer stopping...")
	// tell producer to stop
	p.quit <- true

	// wait in case we are still pushing to the JobQueue
	select {
	case <-p.finished:
		log.Println("JobProducer stopped")
	case <-ctx.Done():
		log.Println("JobProducer stopped while pushing new jobs")
	}
}

type JobConsumer struct {
	Workers  int
	jobQueue chan Job
	wg       sync.WaitGroup
}

func (c *JobConsumer) Start(ctx context.Context) chan<- Job {
	if c.Workers == 0 {
		c.Workers = 10
	}
	c.jobQueue = make(chan Job, c.Workers)

	for i := 0; i < c.Workers; i++ {
		c.wg.Add(1)
		go func(worker int) {
			for job := range c.jobQueue {
				// update a job table here
				deadlineCtx, cancel := context.WithTimeout(ctx, time.Second*5)
				_ = job.Execute(deadlineCtx)
				cancel()
			}
			log.Println(fmt.Sprintf("worker %d stopping", worker))
			c.wg.Done()
		}(i)
	}
	return c.jobQueue
}

func (c *JobConsumer) Stop(ctx context.Context) {
	log.Println("JobConsumer stopping...")
	close(c.jobQueue)

	done := make(chan bool)
	go func() {
		c.wg.Wait()
		done <- true
	}()
	select {
	case <-done:
		log.Println("JobConsumer stopped")
	case <-ctx.Done():
		log.Println("JobConsumer failed to stop correctly")
	}
}
