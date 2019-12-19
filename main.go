package main

import (
	"fmt"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/urfave/cli/v2"
	"github.com/valyala/fastjson"
	"os"
	"strings"
)

var archiverCtx struct {
	tables map[string]bool
}

var pool *pgx.ConnPool
var replacer = strings.NewReplacer(".", "_")
var jsonParser fastjson.Parser

type FeedArchiverArgs struct {
	DbHost       string
	DbUser       string
	DbPassword   string
	DbDatabase   string
	MqConnstring string
	Exchanges    []string
}

func main() {
	app := cli.NewApp()
	app.Name = "feed_archiver"
	app.Authors = []*cli.Author{
		{
			Name:  "simon987",
			Email: "me@simon987.net",
		},
	}
	app.Version = "2.0"

	args := FeedArchiverArgs{}

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "db-host",
			Usage:       "Database host",
			Destination: &args.DbHost,
			Value:       "127.0.0.1",
			EnvVars:     []string{"FA_DB_HOST"},
		},
		&cli.StringFlag{
			Name:        "db-user",
			Usage:       "Database user",
			Destination: &args.DbUser,
			Value:       "feed_archiver",
			EnvVars:     []string{"FA_DB_USER"},
		},
		&cli.StringFlag{
			Name:        "db-password",
			Usage:       "Database password",
			Destination: &args.DbPassword,
			Value:       "feed_archiver",
			EnvVars:     []string{"FA_DB_USER"},
		},
		&cli.StringFlag{
			Name:        "mq-connstring",
			Usage:       "RabbitMQ connection string",
			Destination: &args.MqConnstring,
			Value:       "amqp://guest:guest@localhost:5672/",
			EnvVars:     []string{"FA_MQ_CONNSTRING"},
		},
		&cli.StringSliceFlag{
			Name:     "exchanges",
			Usage:    "RabbitMQ exchanges",
			Required: true,
			EnvVars:  []string{"FA_EXCHANGES"},
		},
	}

	app.Action = func(c *cli.Context) error {

		args.Exchanges = c.StringSlice("exchanges")

		archiverCtx.tables = map[string]bool{}

		logrus.SetLevel(logrus.InfoLevel)

		connPoolConfig := pgx.ConnPoolConfig{
			ConnConfig: pgx.ConnConfig{
				Host:     args.DbHost,
				User:     args.DbUser,
				Password: args.DbPassword,
				Database: args.DbDatabase,
			},
			MaxConnections: 5,
		}

		var err error
		pool, err = pgx.NewConnPool(connPoolConfig)
		if err != nil {
			panic(err)
		}

		return consumeRabbitmqMessage(
			args.MqConnstring,
			args.Exchanges,
			func(delivery amqp.Delivery) error {
				table := routingKeyToTable(delivery.Exchange, delivery.RoutingKey, replacer)
				archive(table, delivery.Body)
				return nil
			},
		)
	}

	err := app.Run(os.Args)
	if err != nil {
		logrus.Fatal(app.OnUsageError)
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
	v, _ := jsonParser.ParseBytes(json)

	arr, err := v.Array()
	if err != nil {
		logrus.WithField("json", string(json)).Error("Message is not an array!")
		return
	}

	for _, item := range arr {
		idValue := item.Get("_id")
		if idValue == nil {
			logrus.WithField("json", string(json)).Error("Item with no _id field!")
			return
		}

		var id interface{}
		if idValue.Type() == fastjson.TypeNumber {
			id, _ = idValue.Int64()
		} else if idValue.Type() == fastjson.TypeString {
			id, _ = idValue.StringBytes()
		}

		_, tableExists := archiverCtx.tables[table]
		if !tableExists {
			createTable(table, idValue.Type())
		}

		logrus.WithFields(logrus.Fields{
			"table": table,
			"id":    idValue,
		}).Debug("Insert row")

		_, err := pool.Exec(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", table), id, item.String())
		if err != nil {
			logrus.WithError(err).Error("Error during insert")
		}
	}
}

func createTable(table string, idType fastjson.Type) {

	logrus.WithFields(logrus.Fields{
		"table": table,
	}).Info("Create table (If not exists)")

	var err error
	var strType string
	if idType == fastjson.TypeNumber {
		strType = "bigint"
	} else {
		strType = "bytea"
	}

	_, err = pool.Exec(fmt.Sprintf("CREATE table IF NOT EXISTS %s ("+
		"id %s PRIMARY KEY,"+
		"archived_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"+
		"data JSONB"+
		")", table, strType))

	if err != nil {
		logrus.WithError(err).Error("Error during create table")
	}

	archiverCtx.tables[table] = true
}

func consumeRabbitmqMessage(host string, exchanges []string, consume func(amqp.Delivery) error) error {

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
