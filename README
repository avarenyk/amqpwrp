# AmqpWRP

pros : handling reconnect, has pool of channels for blocking operations with server. also pool of channels gives more throughput on single connection
cons: ack on each delivery (wanna to make ack after all prefetch received)
todo: wanna add non blocking and non safe publish


# Usage

### Consume
```
stopFunc := amqpwrp.NewConsume(&amqpwrp.ConnConfig{
		Address:     "amqp://guest:guest@localhost:5672/",
		ChannelPool: 10,
	}, &amqpwrp.ConsumeConfig{
		Prefetch: 100,
		Queue:    "queueName",
		Args:     amqp.Table{},
		Callback: func(d amqp.Delivery) (e error, b bool) {
			cl := &RouteCollection{}
			err := json.Unmarshal(d.Body, &cl)
			if err != nil {
				return err, false
			}
			if len(cl.Key) == 0 {
				return fmt.Errorf("empty collection"), false
			}
			channelToPublish <- cl
			return nil, false
		},
	})
```

### Publish

```
pubCh, stopFunc := amqpwrp.NewPublish(&amqpwrp.ConnConfig{
		Address:     "amqp://guest:guest@localhost:5672/",
		ChannelPool: 10,
	}, &amqpwrp.PublishConfig{
		Exchange:  "exchangeName",
		Key:       "queueName",
	})

....

// push message to channel
for _, cp := range cityPairList {
		for _, d := range dateList {
			pubCh <- amqp.Publishing{
				ContentType: "application/json",
				Body:        cp.toJsonWithDate(d),
			}
		}
	}
	// stop is blocking operation, pushing all remaining message and close resources
	stopFunc()
```
