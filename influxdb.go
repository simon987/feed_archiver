package main

import (
	influx "github.com/influxdata/influxdb1-client/v2"
	"github.com/sirupsen/logrus"
	"time"
)

type Monitoring struct {
	influxdb        influx.Client
	points          chan *influx.Point
	connString      string
	bufferSize      int
	retentionPolicy string
	database        string
}

func NewMonitoring(connString string, buffer int) *Monitoring {
	m := new(Monitoring)
	m.bufferSize = buffer
	m.points = make(chan *influx.Point, m.bufferSize)
	m.retentionPolicy = ""
	m.database = "feed_archiver"
	m.influxdb, _ = influx.NewHTTPClient(influx.HTTPConfig{
		Addr: connString,
	})
	go m.asyncWriter()
	return m
}

func (m *Monitoring) asyncWriter() {

	logrus.Info("Started async influxdb writer")

	bp, _ := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:        m.database,
		RetentionPolicy: m.retentionPolicy,
	})

	for point := range m.points {
		bp.AddPoint(point)

		if len(bp.Points()) >= m.bufferSize {
			m.flushQueue(&bp)
		}
	}
	m.flushQueue(&bp)
}

func (m *Monitoring) flushQueue(bp *influx.BatchPoints) {

	err := m.influxdb.Write(*bp)

	if err != nil {
		logrus.WithError(err).Error("influxdb write")
		return
	}

	logrus.WithFields(logrus.Fields{
		"size": len((*bp).Points()),
	}).Info("Wrote points")

	*bp, _ = influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:        m.database,
		RetentionPolicy: m.retentionPolicy,
	})
}

func (m *Monitoring) writeMetricInsertRow(size int, table string) {

	point, _ := influx.NewPoint(
		"insert_row",
		map[string]string{
			"table": table,
		},
		map[string]interface{}{
			"size": size,
		},
		time.Now(),
	)
	m.points <- point
}

func (m *Monitoring) writeMetricUniqueViolation(size int, table string) {

	point, _ := influx.NewPoint(
		"unique_violation",
		map[string]string{
			"table": table,
		},
		map[string]interface{}{
			"size": size,
		},
		time.Now(),
	)
	m.points <- point
}
