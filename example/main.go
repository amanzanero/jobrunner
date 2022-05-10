package main

import (
	"context"
	"github.com/amanzanero/jobrunner"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const MaxWorkers = 50

func main() {
	// setup job consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer := jobrunner.JobConsumer{
		Workers: MaxWorkers,
	}
	jobQueue := consumer.Start(ctx)

	// setup job producer
	producer := jobrunner.JobProducer{
		Reader:   &thingyProducer{},
		JobQueue: jobQueue,
	}
	producer.Start()

	// block on sigterm
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	<-c

	// stop producing jobs
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Second*10)
	defer timeoutCancel()
	producer.Stop(timeoutCtx)
	consumer.Stop(timeoutCtx)
	log.Println("jobrunner has completed")
}

type thingy struct {
}

func (t *thingy) Execute(ctx context.Context) error {
	randomExecutionTime := time.Duration(rand.Int() % 3)
	select {
	case <-ctx.Done():
		log.Println(ctx.Err())
	case <-time.After(time.Second * randomExecutionTime):
	}

	return nil
}

type thingyProducer struct{}

func (t *thingyProducer) Read(_ time.Time, jobs []jobrunner.Job) (int, error) {
	// randomly produce 100
	for i := 0; i < 100; i++ {
		jobs[i] = &thingy{}
	}
	return 100, nil
}
