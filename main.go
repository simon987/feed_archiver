package main

import (
	"fmt"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/valyala/fastjson"
	"strings"
)

var archiverCtx struct {
	tables map[string]bool
}

var pool *pgx.ConnPool
var replacer = strings.NewReplacer(".", "_")
var jsonParser fastjson.Parser


func main() {

	archiverCtx.tables = map[string]bool{}

	logrus.SetLevel(logrus.InfoLevel)

	var err error

	connPoolConfig := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     "127.0.0.1",
			User:     "feed_archiver",
			Password: "feed_archiver",
			Database: "feed_archiver",
		},
		MaxConnections: 5,
	}
	pool, err = pgx.NewConnPool(connPoolConfig)
	if err != nil {
		logrus.Fatalf("Unable to create connection pool", "error", err)
	}

	err = consumeRabbitmqMessage(
		"amqp://guest:guest@localhost:5672/",
		[]string{"reddit", "chan"},
		func(delivery amqp.Delivery) error {
			table := routingKeyToTable(delivery.Exchange, delivery.RoutingKey, replacer)
			archive(table, delivery.Body)
			return nil
		},
	)

	if err != nil {
		logrus.WithError(err).Error("Error during insert")
	}
}

func routingKeyToTable(exchange, routingKey string, replacer *strings.Replacer) string {
	var table string
	if idx := strings.LastIndex(routingKey, "."); idx != -1 {
		table = routingKey[:idx]
	}
	table = replacer.Replace(table)
	return exchange + "_" + table
}

func archive(table string, json []byte) {

	_, tableExists := archiverCtx.tables[table]
	if !tableExists {
		createTable(table)
	}

	v, _ := jsonParser.ParseBytes(json)
	id := v.GetInt64("_id")

	logrus.WithFields(logrus.Fields{
		"table": table,
	}).Debug("Insert row")

	_, err := pool.Exec(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", table), id, json)
	if err != nil {
		logrus.WithError(err).Error("Error during insert")
	}
}

func createTable(table string) {

	logrus.WithFields(logrus.Fields{
		"table": table,
	}).Info("Create table (If not exists)")

	var err error

	_, err = pool.Exec(fmt.Sprintf("CREATE table IF NOT EXISTS %s ("+
		"id bigint PRIMARY KEY,"+
		"archived_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"+
		"data JSONB"+
		")", table))

	if err != nil {
		logrus.WithError(err).Error("Error during create table")
	}

	archiverCtx.tables[table] = true
}

func consumeRabbitmqMessage(host string, exchanges []string, consume func(amqp.Delivery) error) error {

	logrus.Info()
	conn, err := amqp.Dial(host)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for _, exchange := range exchanges {
		err = ch.QueueBind(
			q.Name,
			"#",
			exchange,
			false,
			nil)
		if err != nil {
			return err
		}
		logrus.WithFields(logrus.Fields{
			"exchange": exchange,
		}).Info("Queue bind")
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for d := range msgs {
		err := consume(d)
		if err != nil {
			return err
		}
	}
	return nil
}
