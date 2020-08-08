package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/valyala/fastjson"
	"os"
	"strings"
	"time"
)

var archiverCtx struct {
	tables map[string]bool
}

var pool *pgx.ConnPool
var replacer = strings.NewReplacer(".", "_")

type FeedArchiverArgs struct {
	DbHost     string
	DbUser     string
	DbPassword string
	DbDatabase string
	RedisAddr  string
	Pattern    string
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
	app.Version = "3.0"

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
			EnvVars:     []string{"FA_DB_PASSWORD"},
		},
		&cli.StringFlag{
			Name:        "redis-addr",
			Usage:       "Redis addr",
			Destination: &args.RedisAddr,
			Value:       "localhost:6379",
			EnvVars:     []string{"FA_REDIS_ADDR"},
		},
		&cli.StringFlag{
			Name:        "pattern",
			Usage:       "redis arc pattern",
			Destination: &args.Pattern,
			EnvVars:     []string{"FA_PATTERN"},
		},
	}

	app.Action = func(c *cli.Context) error {

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

		rdb := redis.NewClient(&redis.Options{
			Addr:     args.RedisAddr,
			Password: "",
			DB:       0,
		})

		for i := 1; i <= 5; i++ {
			var parser fastjson.Parser
			go dispatchFromQueue(
				rdb,
				args.Pattern,
				func(message string, key string) error {

					table := routingKeyToTable(key[len(args.Pattern)-1:], replacer)
					archive(parser, table, message)
					return nil
				},
			)
		}
		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		logrus.Error(err)
		logrus.Fatal(app.OnUsageError)
	}
	for {
		time.Sleep(time.Second * 30)
	}
}

func routingKeyToTable(key string, replacer *strings.Replacer) string {
	var table string
	if idx := strings.LastIndex(key, "."); idx != -1 {
		table = key[:idx]
	}
	table = replacer.Replace(table)
	return table
}

func archive(parser fastjson.Parser, table string, json string) {
	item, _ := parser.Parse(json)

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
		if err.(pgx.PgError).Code != "23505" {
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

var keyCache []string = nil

func getKeys(ctx context.Context, rdb *redis.Client, pattern string) []string {

	if keyCache == nil {
		keys, err := rdb.Keys(ctx, pattern).Result()
		if err != nil {
			logrus.WithField("Pattern", pattern).Error("Could not get keys for Pattern")
			return nil
		}

		keyCache = keys
	}

	return keyCache
}

func dispatchFromQueue(rdb *redis.Client, pattern string, consume func(message string, key string) error) error {

	ctx := context.Background()

	for {
		keys := getKeys(ctx, rdb, pattern)
		if len(keys) == 0 {
			time.Sleep(time.Second * 1)
			continue
		}

		rawTask, err := rdb.BLPop(ctx, time.Second*30, keys...).Result()
		if err != nil {
			keyCache = nil
			continue
		}

		err = consume(rawTask[1], rawTask[0])
		if err != nil {
			panic(err)
		}
	}
}
