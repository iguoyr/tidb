package main

import (
	"context"
	"fmt"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"sort"
)

func NewManifest() *plugin.EngineManifest {
	pluginName := "grpc"
	pluginVersion := uint16(1)
	return &plugin.EngineManifest{
		Manifest: plugin.Manifest{
			Kind:    plugin.Engine,
			Name:    pluginName,
			Version: pluginVersion,
			SysVars: map[string]*variable.SysVar{
				pluginName + "_key": {
					Scope: variable.ScopeGlobal,
					Name:  pluginName + "_key",
					Value: "v1",
				},
			},
			OnInit:     OnInit,
			OnShutdown: OnShutdown,
			Validate:   Validate,
		},
		OnReaderOpen:       OnReaderOpen,
		OnReaderNext:       OnReaderNext,
		OnSelectReaderOpen: OnSelectReaderOpen,
		OnSelectReaderNext: OnSelectReaderNext,
	}
}

// Validate implements TiDB plugin's Validate SPI.
func Validate(ctx context.Context, m *plugin.Manifest) error {
	fmt.Println("grpc plugin validate")
	return nil
}

// OnInit implements TiDB plugin's OnInit SPI.
func OnInit(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("grpc init called")
	return nil
}

// OnShutdown implements TiDB plugin's OnShutdown SPI.
func OnShutdown(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("grpc shutdown called")
	return nil
}

var pos = 0

type PrometheusMetric struct {
	SpanKind string
	Duration int64
}

func NewPrometheusMetric(spanKind string, duration int64) PrometheusMetric {
	return PrometheusMetric{
		SpanKind: spanKind,
		Duration: duration,
	}
}

var data = []PrometheusMetric{
	NewPrometheusMetric("GET /api1", 1),
	NewPrometheusMetric("GET /api2", 20),
	NewPrometheusMetric("GET /api3", 300),
	NewPrometheusMetric("GET /api4", 4),
	NewPrometheusMetric("GET /api5", 5000),
}

type MetricSlice []PrometheusMetric

func (s MetricSlice) Len() int {
	return len(s)
}

func (s MetricSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s MetricSlice) Less(i, j int) bool {
	return s[i].Duration < s[j].Duration
}

func OnReaderOpen(ctx context.Context, meta *plugin.ExecutorMeta) error {
	fmt.Println("grpc on reader open called")
	pos = -1
	return nil
}

func OnReaderNext(ctx context.Context, chk *chunk.Chunk, meta *plugin.ExecutorMeta) error {
	fmt.Println("grpc on reader next called")
	chk.Reset()
	pos += 1
	if pos >= len(data) {
		return nil
	}
	MetricsToChk(chk, data[pos], meta)

	return nil
}

var SPos = 0
var Selected []interface{}

func copyData() MetricSlice {
	d := make([]PrometheusMetric, 0, len(data))
	for _, item := range data {
		d = append(d, item)
	}
	return d
}

// 获得selected([]interface)
func OnSelectReaderOpen(ctx context.Context, filters []expression.Expression, meta *plugin.ExecutorMeta) error {
	fmt.Println("grpc on select reader open called")
	SPos = -1
	Selected = make([]interface{}, 0)

	data1 := copyData()
	sort.Sort(sort.Reverse(data1))
	for i, item := range data1 {
		for _, filter := range filters {
			scalarFunction, ok := filter.(*expression.ScalarFunction)
			if !ok {
				continue
			}
			args := scalarFunction.GetArgs()
			logutil.BgLogger().Info("filter name",
				zap.String("name", scalarFunction.FuncName.String()),
				zap.Any("args", args))
			switch scalarFunction.FuncName {
			case model.NewCIStr("eq"):
				logutil.BgLogger().Info("arg",
					zap.String("arg1", args[1].String()))
				if i < 3 {
					logutil.BgLogger().Info("add chunk", zap.String("body", item.SpanKind))
					Selected = append(Selected, item)
				} else {
					logutil.BgLogger().Info("add chunk filter", zap.String("body", item.SpanKind))
				}
			}
		}
	}

	return nil
}

// 根据selected([]interface)注入到chunk中
func OnSelectReaderNext(ctx context.Context, chk *chunk.Chunk,
	filters []expression.Expression, meta *plugin.ExecutorMeta) error {
	fmt.Println("grpc on select reader next called")
	chk.Reset()
	SPos += 1
	if SPos >= len(Selected) {
		return nil
	}

	MetricsToChk(chk, Selected[SPos], meta)
	return nil
}

// TODO 利用反射
func MetricsToChk(chk *chunk.Chunk, doc interface{}, meta *plugin.ExecutorMeta) {
	names := make([]*types.FieldName, 0, len(meta.Columns))
	for _, col := range meta.Columns {
		names = append(names, &types.FieldName{ColName: col.Name})
	}
	metric, ok := doc.(PrometheusMetric)
	if !ok {
		chk.AppendNull(0)
		return
	}
	if idx := expression.FindFieldNameIdxByColName(names, "span_kind"); idx != -1 {
		chk.AppendString(idx, metric.SpanKind)
	}
	if idx := expression.FindFieldNameIdxByColName(names, "duration"); idx != -1 {
		chk.AppendInt64(idx, metric.Duration)
	}
}
