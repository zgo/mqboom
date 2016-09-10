package mqboom

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/streadway/amqp"
)

type result struct {
	err           error
	statusCode    int
	duration      time.Duration
	contentLength int64
}

type Work struct {
	// URI is the AMQP server to be used
	URI  string
	Body []byte

	// N is the total number of requests to make.
	N int

	// C is the concurrency level, the number of concurrent workers to run.
	C int

	// H2 is an option to make HTTP/2 requests
	H2 bool

	// Timeout in seconds.
	Timeout int

	// Qps is the rate limit.
	Qps int

	// Output represents the output type. If "csv" is provided, the
	// output will be dumped as a csv stream.
	Output string

	results chan *result
}

// Run makes all the requests, prints the summary. It blocks until
// all work is done.
func (b *Work) Run() {
	b.results = make(chan *result, b.N)

	start := time.Now()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		// TODO(jbd): Progress bar should not be finalized.
		newReport(b.N, b.results, b.Output, time.Now().Sub(start), false).finalize()
		os.Exit(1)
	}()

	b.run()
	newReport(b.N, b.results, b.Output, time.Now().Sub(start), false).finalize()
	close(b.results)
}

func (b *Work) makeRequest(sess session) error {
	s := time.Now()
	var size int64
	var code int
	err := sess.Publish("", sess.Queue.Name, false, false, amqp.Publishing{Body: b.Body})
	if err == nil {
		size = int64(len(b.Body))
		code = 200
	}
	t := time.Now()
	finish := t.Sub(s)
	b.results <- &result{
		statusCode:    code,
		duration:      finish,
		err:           err,
		contentLength: size,
	}
	return err
}

func (b *Work) do(n int, sessions chan chan session) {
	var throttle <-chan time.Time
	if b.Qps > 0 {
		throttle = time.Tick(time.Duration(1e6/(b.Qps)) * time.Microsecond)
	}

	i := 0
	for session := range sessions {
		pub := <-session

		for ; i < n; i++ {
			if b.Qps > 0 {
				<-throttle
			}
			err := b.makeRequest(pub)
			// Retry failed delivery on the next session
			if err != nil {
				log.Println(err)
				pub.Close()
				break
			}
		}
		if i >= n {
			return
		}
	}
}

func (b *Work) run() {
	var wg sync.WaitGroup
	wg.Add(b.C)

	ctx, done := context.WithCancel(context.Background())
	sessions := redial(ctx, b.URI)

	for i := 0; i < b.C; i++ {
		go func() {
			b.do(b.N/b.C, sessions)
			wg.Done()
		}()
	}
	wg.Wait()
	done()
}
