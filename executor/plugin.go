package executor

import (
	"context"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/util/chunk"
)

type PluginScanExecutor struct {
	baseExecutor
	Table   *model.TableInfo
	Columns []*model.ColumnInfo
	Plugin  *plugin.Plugin
	pm      *plugin.EngineManifest
	meta    *plugin.ExecutorMeta
}

func (e *PluginScanExecutor) Open(ctx context.Context) error {
	e.pm = plugin.DeclareEngineManifest(e.Plugin.Manifest)
	e.meta = &plugin.ExecutorMeta{
		Table:  e.Table,
		Schema: e.baseExecutor.Schema(),
	}
	if e.pm.OnReaderOpen != nil {
		return e.pm.OnReaderOpen(ctx, e.meta)
	}
	return nil
}

func (e *PluginScanExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	err := e.pm.OnReaderNext(ctx, chk, e.meta)
	return err
}

func (e *PluginScanExecutor) Close() error {
	return nil
}

type PluginInsertExec struct {
	baseExecutor
	Plugin  *plugin.Plugin
	pm      *plugin.EngineManifest
	InsertE *InsertExec
	meta    *plugin.ExecutorMeta
}

func (e *PluginInsertExec) Open(ctx context.Context) error {
	e.pm = plugin.DeclareEngineManifest(e.Plugin.Manifest)
	e.meta = &plugin.ExecutorMeta{
		Table:  e.InsertE.Table.Meta(),
		Schema: e.baseExecutor.Schema(),
	}
	return e.pm.OnInsertOpen(ctx, e.meta)
}

func (e *PluginInsertExec) Next(ctx context.Context, req *chunk.Chunk) error {
	return e.pm.OnInsertNext(ctx, e.InsertE.Lists, e.meta)
}

func (e *PluginInsertExec) Close() error {
	return e.pm.OnInsertClose(e.meta)
}

type PluginSelectionExec struct {
	baseExecutor
	Plugin  *plugin.Plugin
	pm      *plugin.EngineManifest
	filter  []expression.Expression
	meta    *plugin.ExecutorMeta
	Table   *model.TableInfo
	Columns []*model.ColumnInfo
}

func (e *PluginSelectionExec) Open(ctx context.Context) error {
	e.pm = plugin.DeclareEngineManifest(e.Plugin.Manifest)
	e.meta = &plugin.ExecutorMeta{
		Table:   e.Table,
		Schema:  e.baseExecutor.Schema(),
		Columns: e.Columns,
	}
	if e.pm.OnSelectReaderOpen != nil {
		return e.pm.OnSelectReaderOpen(ctx, e.filter, e.meta)
	}
	return nil
}

func (e *PluginSelectionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	for _, v := range e.baseExecutor.Schema().Columns {
		fmt.Println(spew.Sdump(v.ID))
	}
	return e.pm.OnSelectReaderNext(ctx, chk, e.filter, e.meta)
}
