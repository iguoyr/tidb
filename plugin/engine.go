package plugin

import (
	"context"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
)

type EngineManifest struct {
	Manifest
	OnInsertOpen  func(ctx context.Context, meta *ExecutorMeta) error
	OnInsertNext  func(ctx context.Context, rows [][]expression.Expression, meta *ExecutorMeta) error
	OnInsertClose func(meta *ExecutorMeta) error

	OnReaderOpen  func(ctx context.Context, meta *ExecutorMeta) error
	OnReaderNext  func(ctx context.Context, chk *chunk.Chunk, meta *ExecutorMeta) error
	OnReaderClose func(meta ExecutorMeta)

	OnSelectReaderNext func(ctx context.Context, chk *chunk.Chunk, filter []expression.Expression, meta *ExecutorMeta) error
	OnSelectReaderOpen func(ctx context.Context, filter []expression.Expression, meta *ExecutorMeta) error

	OnDropTable   func(tb *model.TableInfo) error
	OnCreateTable func(tb *model.TableInfo) error
}

type ExecutorMeta struct {
	Table       *model.TableInfo
	Schema      *expression.Schema
	Columns        []*model.ColumnInfo
}

func HasEngine(name string) bool {
	p := Get(Engine, name)
	if p != nil {
		return true
	}

	return false
}
