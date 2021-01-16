package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"strconv"
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
	Id   int64
	Body string
}

func NewEsDoc(ip string, status int, id int64) EsDoc {
	var msg string
	switch status {
	case 200:
		msg = "access web"
	case 401:
		msg = "unauthorized"
	case 500:
		msg = "Server Error"
	default:
		msg = "UNKNOWN"
	}

	return EsDoc{
		Id:   id,
		Body: fmt.Sprintf(`{"status": %d, "IP": "%s", "message": "ip:%s is %s"}`, status, ip, ip, msg),
	}
}

var data = []EsDoc{
	NewEsDoc("1.0.0.202", 500, 1),
	NewEsDoc("2.0.0.202", 401, 2),
	NewEsDoc("3.0.0.202", 200, 3),
	NewEsDoc("3.0.0.201", 500, 4),
	NewEsDoc("1.0.0.220", 500, 5),
	NewEsDoc("1.0.0.221", 200, 6),
	NewEsDoc("2.0.0.222", 500, 7),
	NewEsDoc("1.0.0.224", 200, 8),
	NewEsDoc("1.0.0.225", 200, 9),
	NewEsDoc("2.0.0.223", 401, 10),
}

func OnReaderOpen(ctx context.Context, meta *plugin.ExecutorMeta) error {
	pos = -1
	return nil
}

func OnReaderNext(ctx context.Context, chk *chunk.Chunk, meta *plugin.ExecutorMeta) error {
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
	SPos = -1
	Selected = []EsDoc{}

	for _, item := range data {
		for _, filter := range filters {
			logutil.BgLogger().Info("filter name",
				zap.String("name", filter.(*expression.ScalarFunction).FuncName.String()))
			switch filter.(*expression.ScalarFunction).FuncName {
			case model.NewCIStr("eq"):
				var body map[string]interface{}
				_ = json.Unmarshal([]byte(item.Body), &body)
				status := strconv.Itoa(int(body["status"].(float64)))
				if status != "200" {
					logutil.BgLogger().Info("add chunk", zap.String("body", item.Body))
					Selected = append(Selected, item)
				} else {
					logutil.BgLogger().Info("add chunk filter", zap.String("body", item.Body))
				}
			}
		}
	}

	return nil
}

func DocsToChk(chk *chunk.Chunk, doc EsDoc, meta *plugin.ExecutorMeta) {
	names := make([]*types.FieldName, 0, len(meta.Columns))
	for _, col := range meta.Columns {
		names = append(names, &types.FieldName{ColName: col.Name})
	}
	if idx := expression.FindFieldNameIdxByColName(names, "id"); idx != -1 {
		chk.AppendInt64(idx, doc.Id)
	}
	if idx := expression.FindFieldNameIdxByColName(names, "body"); idx != -1 {
		chk.AppendString(idx, doc.Body)
	}
}

func OnSelectReaderNext(ctx context.Context, chk *chunk.Chunk,
	filters []expression.Expression, meta *plugin.ExecutorMeta) error {
	chk.Reset()
	SPos += 1
	if SPos >= len(Selected) {
		return nil
	}

	DocsToChk(chk, Selected[SPos], meta)
	return nil
}
