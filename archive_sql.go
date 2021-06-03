package main

import (
	"fmt"
	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
	"strings"
)

func routingKeyToTable(key string, replacer *strings.Replacer) string {
	var table string
	if idx := strings.LastIndex(key, "."); idx != -1 {
		table = key[:idx]
	}
	table = replacer.Replace(table)
	return table
}

func archiveSql(record *Record) error {

	table := routingKeyToTable(record.RoutingKey, replacer)
	archiverCtx.m.RLock()
	_, tableExists := archiverCtx.tables[table]
	archiverCtx.m.RUnlock()

	if !tableExists {
		createTable(table, record.IdType)
	}

	logrus.WithFields(logrus.Fields{
		"table": table,
		"id":    record.IdValue,
	}).Debug("Insert row")

	itemString := record.Item.String()
	messageSize := len(itemString)

	_, err := pool.Exec(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", table), record.IdValue, itemString)
	if err != nil {
		if err.(pgx.PgError).Code != "23505" {
			logrus.WithError(err).Error("Error during insert")
		} else if archiverCtx.Monitoring != nil {
			archiverCtx.Monitoring.writeMetricUniqueViolation(messageSize, table)
		}
	} else if archiverCtx.Monitoring != nil {
		archiverCtx.Monitoring.writeMetricInsertRow(messageSize, table)
	}

	return nil
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
		"archived_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,"+
		"data JSONB NOT NULL"+
		")", table, strType))

	if err != nil {
		logrus.WithError(err).Error("Error during create table")
	}

	archiverCtx.m.Lock()
	archiverCtx.tables[table] = true
	archiverCtx.m.Unlock()
}
