package publisher

import (
	"sync"

	"github.com/elastic/beats/filebeat/input"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
	"runtime"
)
const MAX_PUBLISH_CNT int = 5

type syncLogPublisher struct {
	pub    publisher.Publisher
	client [MAX_PUBLISH_CNT]publisher.Client
	in     chan []*input.Event
	out    SuccessLogger

	done chan struct{}
	wg   sync.WaitGroup
}

func newSyncLogPublisher(
	in chan []*input.Event,
	out SuccessLogger,
	pub publisher.Publisher,
) *syncLogPublisher {
	return &syncLogPublisher{
		in:   in,
		out:  out,
		pub:  pub,
		done: make(chan struct{}),
	}
}

func (p *syncLogPublisher) Start() {
	for index := 0; index < MAX_PUBLISH_CNT; index++ {
		p.client[index] = p.pub.Connect()
	}

	//p.client = p.pub.Connect()

	p.wg.Add(MAX_PUBLISH_CNT)
	runtime.GOMAXPROCS(MAX_PUBLISH_CNT)
	for index := 0; index < MAX_PUBLISH_CNT; index++ {
		go func(index int) {
			defer p.wg.Done()

			logp.Info("Start sending events to output")
			defer logp.Debug("publisher", "Shutting down sync publisher")

			for {
				err := p.Publish(index)
				if err != nil {
					return
				}
			}
		}(index)
	}

}

func (p *syncLogPublisher) Publish(index int) error {
	var events []*input.Event
	select {
	case <-p.done:
		return sigPublisherStop
	case events = <-p.in:
	}

	dataEvents, meta := getDataEvents(events)
	ok := p.client[index].PublishEvents(dataEvents, publisher.Sync, publisher.Guaranteed,
		publisher.MetadataBatch(meta))
	if !ok {
		// PublishEvents will only returns false, if p.client has been closed.
		return sigPublisherStop
	}

	// TODO: move counter into logger?
	logp.Debug("publish", "Events sent: %d", len(events))
	eventsSent.Add(int64(len(events)))

	// Tell the logger that we've successfully sent these events
	ok = p.out.Published(events)
	if !ok {
		// stop publisher if successfully send events can not be logged anymore.
		return sigPublisherStop
	}
	return nil
}

func (p *syncLogPublisher) Stop() {
	for _, client := range p.client {
		client.Close()

	}
	close(p.done)
	p.wg.Wait()
}
