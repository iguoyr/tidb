package main

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func NewManifest() *plugin.EngineManifest {
	pluginName := "elasticsearch"
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
	fmt.Println("es plugin validate")
	return nil
}

// OnInit implements TiDB plugin's OnInit SPI.
func OnInit(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("es init called")
	return nil
}

// OnShutdown implements TiDB plugin's OnShutdown SPI.
func OnShutdown(ctx context.Context, manifest *plugin.Manifest) error {
	fmt.Println("es shutdown called")
	return nil
}

var pos = 0

type EsDoc struct {
	Timestamp string
	SpanID    string
	TraceID   string
	SpanKind  string
	Duration  string
}

func NewEsDoc(timestamp, spanId, traceId, spanKind, duration string) EsDoc {
	return EsDoc{
		Timestamp: timestamp,
		SpanKind:  spanKind,
		SpanID:    spanId,
		TraceID:   traceId,
		Duration:  duration,
	}
}

var data = []EsDoc{
	NewEsDoc("08:00:01",
		"56453a04-4846-497e-932f-064705fbb7dd",
		"750dc35a-3eaa-4d13-bfdb-3a834f05a538",
		"HashJoin_18(Build)",
		"8327"),
	NewEsDoc("08:00:01",
		"6793231e-33ec-4321-aac9-0d492db6d944",
		"750dc35a-3eaa-4d13-bfdb-3a834f05a538",
		"TableReader_21(Build)",
		"3342"),
	NewEsDoc("08:00:01",
		"70b18c54-ceaf-45d3-bcd4-c9ed36093ccf",
		"750dc35a-3eaa-4d13-bfdb-3a834f05a538",
		"Selection_20",
		"7"),
	NewEsDoc("08:00:01",
		"e553feb6-65c6-4a28-b88a-7ddc6d39a70b",
		"750dc35a-3eaa-4d13-bfdb-3a834f05a538",
		"TableFullScan_19",
		"726"),
}

func OnReaderOpen(ctx context.Context, meta *plugin.ExecutorMeta) error {
	fmt.Println("es OnReaderOpen called")
	pos = -1
	return nil
}

func OnReaderNext(ctx context.Context, chk *chunk.Chunk, meta *plugin.ExecutorMeta) error {
	fmt.Println("es OnReaderNext called")
	chk.Reset()
	pos += 1
	if pos >= len(data) {
		return nil
	}
	DocsToChk(chk, data[pos], meta)

	return nil
}

var SPos = 0
var Selected []EsDoc

func OnSelectReaderOpen(ctx context.Context, filters []expression.Expression, meta *plugin.ExecutorMeta) error {
	fmt.Println("es OnSelectReaderOpen called")
	SPos = -1
	//Selected = []EsDoc{}
	//
	//for _, item := range data {
	//	for _, filter := range filters {
	//		logutil.BgLogger().Info("filter name",
	//			zap.String("name", filter.(*expression.ScalarFunction).FuncName.String()))
	//		switch filter.(*expression.ScalarFunction).FuncName {
	//		case model.NewCIStr("eq"):
	//			if item.TraceID == "750dc35a-3eaa-4d13-bfdb-3a834f05a538" {
	//				logutil.BgLogger().Info("add chunk", zap.String("span_id", item.SpanID))
	//				Selected = append(Selected, item)
	//			} else {
	//				logutil.BgLogger().Info("add chunk filter", zap.String("span_id", item.SpanID))
	//			}
	//		}
	//	}
	//}

	return nil
}

func DocsToChk(chk *chunk.Chunk, doc EsDoc, meta *plugin.ExecutorMeta) {
	names := make([]*types.FieldName, 0, len(meta.Columns))
	for _, col := range meta.Columns {
		names = append(names, &types.FieldName{ColName: col.Name})
	}
	//if len(names) == 0 {
	//	for _, col := range meta.Table.Columns {
	//		names = append(names, &types.FieldName{ColName: col.Name})
	//	}
	//}
	logutil.BgLogger().Info("columns", zap.Any("names", names))
	if idx := expression.FindFieldNameIdxByColName(names, "timestamp"); idx != -1 {
		chk.AppendString(idx, doc.Timestamp)
	}
	if idx := expression.FindFieldNameIdxByColName(names, "span_kind"); idx != -1 {
		chk.AppendString(idx, doc.SpanKind)
	}
	if idx := expression.FindFieldNameIdxByColName(names, "span_id"); idx != -1 {
		chk.AppendString(idx, doc.SpanID)
	}
	if idx := expression.FindFieldNameIdxByColName(names, "trace_id"); idx != -1 {
		chk.AppendString(idx, doc.TraceID)
	}
	if idx := expression.FindFieldNameIdxByColName(names, "duration"); idx != -1 {
		chk.AppendString(idx, doc.Duration)
	}
	if idx := expression.FindFieldNameIdxByColName(names, "query"); idx != -1 {
		chk.AppendNull(idx)
	}
}

func OnSelectReaderNext(ctx context.Context, chk *chunk.Chunk,
	filters []expression.Expression, meta *plugin.ExecutorMeta) error {
	fmt.Println("es OnSelectReaderNext called")
	chk.Reset()
	SPos += 1
	if SPos >= len(data) {
		return nil
	}

	DocsToChk(chk, data[SPos], meta)
	return nil
}
