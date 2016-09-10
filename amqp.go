package mqboom

import (
	"crypto/sha1"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"golang.org/x/net/context"

	"github.com/streadway/amqp"
)

var (
	amqpConfig = amqp.Config{
		Dial: (&net.Dialer{
			KeepAlive: 30 * time.Second,
			Timeout:   30 * time.Second,
		}).Dial,
		Heartbeat: 10 * time.Second,
	}
)

type session struct {
	*amqp.Connection
	*amqp.Channel

	Confirms chan amqp.Confirmation
	Queue    amqp.Queue
}

func (s session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

func dial(uri string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.DialConfig(uri, amqpConfig)
	if err != nil {
		return nil, nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, channel, nil
}

func identity() string {
	hostname, err := os.Hostname()
	h := sha1.New()
	fmt.Fprint(h, hostname)
	fmt.Fprint(h, err)
	fmt.Fprint(h, os.Getpid())
	return fmt.Sprintf("%x", h.Sum(nil))
}

func redial(ctx context.Context, url string) chan chan session {
	sessions := make(chan chan session)

	go func() {
		sess := make(chan session)
		defer close(sessions)

		for {
			select {
			case <-ctx.Done():
				return
			case sessions <- sess:
			}

			conn, ch, err := dial(url)
			if err != nil {
				log.Fatalf("cannot (re)dial: %v: %q", err, url)
			}

			confirm := make(chan amqp.Confirmation, 1024)
			if err := ch.Confirm(false); err != nil {
				log.Printf("publisher confirms not supported")
				close(confirm) // confirms not supported, simulate by always nacking
			} else {
				go func() {
					for confirmed := range confirm {
						if !confirmed.Ack {
							log.Printf("nack message %d", confirmed.DeliveryTag)
						}
					}
				}()
				ch.NotifyPublish(confirm)
			}

			q, err := ch.QueueDeclare("", false, true, true, false, nil)
			if err != nil {
				log.Fatalf("cannot declare: %v", err)
			}

			select {
			case sess <- session{conn, ch, confirm, q}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return sessions
}
