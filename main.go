package main

import (
	"context"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/valyala/fastjson"
	"os"
	"strings"
	"sync"
	"time"
)

var archiverCtx struct {
	tables      map[string]bool
	m           sync.RWMutex
	ArchiveFunc func(*Record) error
	Monitoring  *Monitoring
	esIndex     string // TODO: based on routing key?
}

type Record struct {
	Item       *fastjson.Value
	IdValue    interface{}
	IdType     fastjson.Type
	RoutingKey string
}

var pool *pgx.ConnPool
var es *elasticsearch.Client
var replacer = strings.NewReplacer(".", "_")

type FeedArchiverArgs struct {
	DbHost         string
	DbUser         string
	DbPassword     string
	DbDatabase     string
	EsHost         string
	EsUser         string
	EsPassword     string
	EsIndex        string
	ArchiveTarget  string
	RedisAddr      string
	RedisPassword  string
	Pattern        string
	Threads        int
	InfluxDb       string
	InfluxDbBuffer int
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
	app.Version = "4.0"

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
			Name:        "redis-password",
			Usage:       "Redis password",
			Destination: &args.RedisPassword,
			Value:       "",
			EnvVars:     []string{"FA_REDIS_PASSWORD"},
		},
		&cli.StringFlag{
			Name:        "pattern",
			Usage:       "redis arc pattern",
			Destination: &args.Pattern,
			EnvVars:     []string{"FA_PATTERN"},
		},
		&cli.IntFlag{
			Name:        "threads",
			Usage:       "number of threads",
			Value:       5,
			Destination: &args.Threads,
			EnvVars:     []string{"FA_THREADS"},
		},
		&cli.StringFlag{
			Name:        "influxdb",
			Usage:       "Influxdb connection string",
			Value:       "",
			Destination: &args.InfluxDb,
			EnvVars:     []string{"FA_INFLUXDB"},
		},
		&cli.IntFlag{
			Name:        "influxdb-buffer",
			Usage:       "Influxdb buffer size",
			Value:       500,
			Destination: &args.InfluxDbBuffer,
			EnvVars:     []string{"FA_INFLUXDB_BUFFER"},
		},
		&cli.StringFlag{
			Name:        "es-host",
			Usage:       "Elasticsearch host",
			Destination: &args.EsHost,
			Value:       "http://localhost:9200",
			EnvVars:     []string{"FA_ES_HOST"},
		},
		&cli.StringFlag{
			Name:        "es-user",
			Usage:       "Elasticsearch username",
			Destination: &args.EsUser,
			Value:       "elastic",
			EnvVars:     []string{"FA_ES_USER"},
		},
		&cli.StringFlag{
			Name:        "es-password",
			Usage:       "Elasticsearch password",
			Destination: &args.EsPassword,
			Value:       "",
			EnvVars:     []string{"FA_ES_PASSWORD"},
		},

		// TODO: Based on routing key?
		&cli.StringFlag{
			Name:        "es-index",
			Usage:       "Elasticsearch index",
			Destination: &args.EsIndex,
			Value:       "feed_archiver",
			EnvVars:     []string{"FA_ES_INDEX"},
		},
		&cli.StringFlag{
			Name:        "target",
			Usage:       "Either 'es' or 'sql'",
			Destination: &args.ArchiveTarget,
			Value:       "sql",
			EnvVars:     []string{"FA_TARGET"},
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
			MaxConnections: args.Threads,
		}

		if args.ArchiveTarget == "sql" {
			var err error
			pool, err = pgx.NewConnPool(connPoolConfig)
			if err != nil {
				panic(err)
			}
			archiverCtx.ArchiveFunc = archiveSql
		} else {
			es, _ = elasticsearch.NewDefaultClient()
			archiverCtx.ArchiveFunc = archiveEs
			archiverCtx.esIndex = args.EsIndex
		}

		archiverCtx.Monitoring = nil
		if args.InfluxDb != "" {
			archiverCtx.Monitoring = NewMonitoring(args.InfluxDb, args.InfluxDbBuffer)
		}

		rdb := redis.NewClient(&redis.Options{
			Addr:     args.RedisAddr,
			Password: args.RedisPassword,
			DB:       0,
		})

		for i := 1; i <= args.Threads; i++ {
			var parser fastjson.Parser
			go dispatchFromQueue(
				rdb,
				args.Pattern,
				func(message string, key string) error {

					item, _ := parser.Parse(message)

					id := item.Get("_id")
					if id == nil {
						logrus.WithField("json", key).Error("Item with no _id field!")
						return nil
					}

					var idValue interface{}

					if id.Type() == fastjson.TypeNumber {
						idValue, _ = id.Int64()
					} else if id.Type() == fastjson.TypeString {
						idValue, _ = id.StringBytes()
					}

					return archiverCtx.ArchiveFunc(&Record{
						Item:       item,
						IdType:     id.Type(),
						IdValue:    idValue,
						RoutingKey: key[len(args.Pattern)-1:],
					})
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

var keyCache []string = nil

// BLPOP with too many keys is slow!
const maxKeys = 30

func getKeys(ctx context.Context, rdb *redis.Client, pattern string) []string {

	if keyCache == nil {
		var cur uint64 = 0
		var keyRes []string
		var keys []string
		var err error

		for {
			keyRes, cur, err = rdb.Scan(ctx, cur, pattern, 10).Result()
			if err != nil {
				logrus.
					WithError(err).
					WithField("Pattern", pattern).
					Error("Could not get keys for Pattern")
				return nil
			}

			if cur == 0 || len(keys) >= maxKeys {
				break
			}

			keys = append(keys, keyRes...)
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
			keyCache = nil
			continue
		}

		rawTask, err := rdb.BLPop(ctx, time.Second, keys...).Result()
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
