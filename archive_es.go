package main

import (
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fastjson"
	"strconv"
	"strings"
)

func routingKeyToIndex(key string) string {

	if idx := strings.Index(key, "."); idx != -1 {
		project := key[:idx]
		return fmt.Sprintf("%s-data", project)
	}

	panic(fmt.Sprintf("Cannot get ES index name for key %s", key))
}

func archiveEs(record *Record) error {

	var stringId string

	if record.IdType == fastjson.TypeNumber {
		stringId = strconv.FormatInt(record.IdValue.(int64), 10)
	} else if record.IdType == fastjson.TypeString {
		stringId = string(record.IdValue.([]uint8))
	}

	index := routingKeyToIndex(record.RoutingKey)

	// Will cause error in ES if we specify the _id field in the document body
	record.Item.Del("_id")

	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: stringId,
		Body:       strings.NewReader(record.Item.String()),
		Refresh:    "false",
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"id":   record.IdValue,
			"item": record.Item.String(),
		}).WithError(err).Error("Request error during document index")
		return nil
	}

	if res.IsError() {
		logrus.WithFields(logrus.Fields{
			"id":     stringId,
			"status": res.String(),
			"item":   record.Item.String(),
		}).Error("ES error during document index")
	}

	logrus.WithFields(logrus.Fields{
		"id":    stringId,
		"index": index,
	}).Debug("Insert document")

	if archiverCtx.Monitoring != nil {
		archiverCtx.Monitoring.writeMetricIndexDoc(index)
	}

	_ = res.Body.Close()

	return nil
}
