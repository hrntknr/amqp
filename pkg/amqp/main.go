package amqp

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	conn *amqp.Connection
}

type Fanout struct {
	conn     *amqp.Connection
	exchange string
}

type RPC struct {
	conn     *amqp.Connection
	exchange string
	key      string
}

type Publisher struct {
	ch       *amqp.Channel
	exchange string
	key      string
}

type Consumer struct {
	Ch <-chan Delivery
}

type Caller struct {
	ch       *amqp.Channel
	exchange string
	key      string
	lock     sync.Mutex
	chs      map[string]chan Delivery
	callback string
}

type Server struct {
	ch   *amqp.Channel
	msgs <-chan amqp.Delivery
}

type Publishing amqp.Publishing
type Delivery amqp.Delivery

func NewClient(url string) (*Client, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func (c *Client) NewFanout(exchange string) *Fanout {
	return &Fanout{conn: c.conn, exchange: exchange}
}
func (c *Client) NewRPC(exchange string, key string) *RPC {
	return &RPC{conn: c.conn, exchange: exchange, key: key}
}

func (c *Fanout) Publisher() (*Publisher, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.ExchangeDeclare(c.exchange, "fanout", true, false, false, false, nil); err != nil {
		return nil, err
	}
	return &Publisher{
		ch:       ch,
		exchange: c.exchange,
		key:      "",
	}, nil
}

func (c *Fanout) Consumer(name *string) (*Consumer, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.ExchangeDeclare(c.exchange, "fanout", true, false, false, false, nil); err != nil {
		return nil, err
	}
	var q amqp.Queue
	if name == nil {
		q, err = ch.QueueDeclare("", false, true, true, false, nil)
	} else {
		q, err = ch.QueueDeclare(fmt.Sprintf("%s.%s", c.exchange, *name), true, false, false, false, nil)
	}
	if err != nil {
		return nil, err
	}
	if err := ch.QueueBind(q.Name, "", c.exchange, false, nil); err != nil {
		return nil, err
	}
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	msgsCh := make(chan Delivery)
	go func() {
		for msg := range msgs {
			msgsCh <- Delivery(msg)
		}
		close(msgsCh)
	}()
	return &Consumer{
		Ch: msgsCh,
	}, nil
}

func (c *RPC) Caller() (*Caller, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.ExchangeDeclare(c.exchange, "direct", true, false, false, false, nil); err != nil {
		return nil, err
	}
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return nil, err
	}
	if err := ch.QueueBind(q.Name, q.Name, c.exchange, false, nil); err != nil {
		return nil, err
	}
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	chs := map[string]chan Delivery{}
	go func() {
		for msg := range msgs {
			if chs, ok := chs[msg.CorrelationId]; ok {
				chs <- Delivery(msg)
			}
		}
	}()
	return &Caller{
		ch:       ch,
		exchange: c.exchange,
		key:      c.key,
		lock:     sync.Mutex{},
		chs:      chs,
		callback: q.Name,
	}, nil
}

func (c *RPC) Server() (*Server, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.ExchangeDeclare(c.exchange, "direct", true, false, false, false, nil); err != nil {
		return nil, err
	}
	q, err := ch.QueueDeclare(fmt.Sprintf("%s.%s", c.exchange, c.key), true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	if err := ch.QueueBind(q.Name, c.key, c.exchange, false, nil); err != nil {
		return nil, err
	}
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	return &Server{
		ch:   ch,
		msgs: msgs,
	}, nil
}

func (p *Publisher) Publish(msg Publishing) error {
	return p.ch.Publish(p.exchange, p.key, false, false, amqp.Publishing(msg))
}

func (c *Caller) Call(ctx context.Context, msg Publishing) (Delivery, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return Delivery{}, err
	}
	msg.CorrelationId = id.String()
	msg.ReplyTo = c.callback
	ch := make(chan Delivery)
	c.lock.Lock()
	c.chs[msg.CorrelationId] = ch
	c.lock.Unlock()
	defer delete(c.chs, msg.CorrelationId)
	if err := c.ch.Publish(c.exchange, c.key, false, false, amqp.Publishing(msg)); err != nil {
		return Delivery{}, err
	}
	select {
	case <-ctx.Done():
		return Delivery{}, ctx.Err()
	case msg := <-ch:
		return msg, nil
	}
}

func (s *Server) Serve(ctx context.Context, handler func(Delivery) Publishing) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-s.msgs:
			ret := amqp.Publishing(handler(Delivery(msg)))
			ret.CorrelationId = msg.CorrelationId
			if err := s.ch.Publish(msg.Exchange, msg.ReplyTo, false, false, ret); err != nil {
				return err
			}
		}
	}
}
