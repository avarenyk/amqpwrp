package amqpwrp

import (
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

const (
	reconnectDelay = 5 * time.Second
	heartbeat      = 5 * time.Second
)

type connection struct {
	config         *ConnConfig
	channelFactory channelFactory

	conn *amqp.Connection
	wg   *sync.WaitGroup
	stop chan bool

	pubCh chan amqp.Publishing
}

type channelFactory interface {
	create(*connection, chan bool)
}

type StopFunc func()

type ConnConfig struct {
	Address     string
	ChannelPool int
}

type ConsumeConfig struct {
	Prefetch  int
	Queue     string
	Consumer  string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	Args      amqp.Table
	Callback  func(d amqp.Delivery) (error, bool)
}

func (f *ConsumeConfig) create(c *connection, chanReopen chan bool) {
	var (
		err             error
		ch              *amqp.Channel
		deliveryChannel <-chan amqp.Delivery
	)
	defer func() {
		ch.Close()
		c.wg.Done()
		if err != nil {
			log.Printf("[amqpwrp] %s\n", err.Error())
			// wait reconnectDelay and send reopen signal
			time.Sleep(reconnectDelay)
			chanReopen <- true
		}
	}()

	ch, err = c.conn.Channel()
	if err != nil {
		return
	}

	err = ch.Qos(f.Prefetch, 0, false)
	if err != nil {
		return
	}

	deliveryChannel, err = ch.Consume(f.Queue, f.Consumer, f.autoAck, f.exclusive, f.noLocal, f.noWait, f.Args)
	if err != nil {
		return
	}

	for d := range deliveryChannel {
		processError, requeue := f.Callback(d)
		if processError != nil {
			log.Printf("[amqpwrp] error on processing msg : %s \n", processError)
			err = d.Nack(false, requeue)
		} else {
			err = d.Ack(false)
		}
		if err != nil {
			return
		}
	}
}

type PublishConfig struct {
	Exchange  string
	Key       string
	mandatory bool
	immediate bool
}

func (f *PublishConfig) create(c *connection, chanReopen chan bool) {
	var (
		err error
		ch  *amqp.Channel
	)
	defer func() {
		log.Println("[amqpwrp] closing publish channel")
		ch.Close()
		c.wg.Done()
		if err != nil {
			log.Printf("[amqpwrp] %s\n", err.Error())
			// wait reconnectDelay and send reopen signal
			time.Sleep(reconnectDelay)
			chanReopen <- true
		}
	}()

	log.Println("[amqpwrp] creating publish channel")

	ch, err = c.conn.Channel()
	if err != nil {
		return
	}

	for {
		select {
		case msg, ok := <-c.pubCh:
			if !ok {
				return
			}
			err = ch.Publish(f.Exchange, f.Key, f.mandatory, f.immediate, msg)
			if err != nil {
				return
			}

		case <-c.stop:
			return
		}
	}
}

func handleReconnect(c *connection, stopped chan bool) {
	defer func() {
		close(stopped)
	}()
connectLoop:
	for {
		connected, closed := c.connect()
		if !connected {
			select {
			case <-time.After(reconnectDelay):
				continue connectLoop
			case <-c.stop:
				return
			}
		}
		select {
		case <-closed:
			time.Sleep(reconnectDelay)
		case <-c.stop:
			c.close()
			return
		}
	}
}

func (c *connection) connect() (bool, <-chan bool) {
	var err error
	defer func() {
		if err != nil {
			log.Printf("[amqpwrp] %s\n", err.Error())
		}
	}()

	log.Printf("[amqpwrp] connecting with %s\n", c.config.Address)
	c.conn, err = amqp.DialConfig(c.config.Address, amqp.Config{Heartbeat: heartbeat})
	if err != nil {
		return false, nil
	}

	// listen to close on connection
	closed := make(chan bool)
	go func() {
		connErrCh := make(chan *amqp.Error)
		c.conn.NotifyClose(connErrCh)

		// wait connection to be closed
		// if connection closed gracefully err will be nil
		err := <-connErrCh

		if err != nil {
			log.Printf("[amqpwrp] %s\n", err)
		}

		close(closed)
	}()

	// reopen channels in pool if any was closed
	// stop reopening when connection is closed
	chanReopen := make(chan bool, c.config.ChannelPool)
	go func() {
		for {
			select {
			case <-chanReopen:
				c.wg.Add(1)
				go c.channelFactory.create(c, chanReopen)
			case <-closed:
				return
			}
		}
	}()

	for i := 0; i < c.config.ChannelPool; i++ {
		chanReopen <- true
	}

	return true, closed
}

// Gracefully close
func (c *connection) close() {
	var err error

	log.Println("[amqpwrp] gracefully connection close...")
	c.wg.Wait()

	err = c.conn.Close()
	if err != nil {
		log.Printf("[amqpwrp] %s\n", err)
	}
}

func NewConsume(connConfig *ConnConfig, consumeConfig *ConsumeConfig) StopFunc {

	conn := &connection{
		config:         connConfig,
		channelFactory: consumeConfig,
		wg:             &sync.WaitGroup{},
		stop:           make(chan bool),
	}

	stopped := make(chan bool)
	go handleReconnect(conn, stopped)

	return func() {
		close(conn.stop)
		<-stopped
	}
}

func NewPublish(c *ConnConfig, p *PublishConfig) (chan<- amqp.Publishing, StopFunc) {

	pubCh := make(chan amqp.Publishing)

	conn := &connection{
		config:         c,
		channelFactory: p,
		pubCh:          pubCh,
		wg:             &sync.WaitGroup{},
		stop:           make(chan bool),
	}

	stopped := make(chan bool)
	go handleReconnect(conn, stopped)

	return pubCh, func() {
		close(conn.stop)
		<-stopped
		close(pubCh)
	}
}
