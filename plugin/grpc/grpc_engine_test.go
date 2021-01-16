package main

import (
	"context"
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"testing"
)

type baseTestSuite struct {
	cluster cluster.Cluster
	store   kv.Storage
	domain  *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

func (s *baseTestSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	var err error
	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	session.SetSchemaLease(0)
	session.DisableStats4Test()

	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *baseTestSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

var _ = Suite(&testPlugin{&baseTestSuite{}})

type testPlugin struct{ *baseTestSuite }

func TestPlugin(t *testing.T) {
	TestingT(t)
}

func (s *testPlugin) TestPlugin(c *C) {
	var result *testkit.Result
	tk := testkit.NewTestKit(c, s.store)

	ctx := context.Background()
	var pluginVarNames []string
	pluginName := "grpc"
	pluginVersion := uint16(1)
	cfg := plugin.Config{
		Plugins:        []string{fmt.Sprintf("%s-%d", pluginName, pluginVersion)},
		PluginDir:      "",
		PluginVarNames: &pluginVarNames,
	}

	// setup load test hook.
	plugin.SetLoadFn(NewManifest())
	defer func() {
		plugin.UnsetLoadFn()
	}()

	err := plugin.Load(ctx, cfg)
	if err != nil {
		panic(err)
	}

	// load and start TiDB domain.
	err = plugin.Init(ctx, cfg)
	if err != nil {
		panic(err)
	}

	tk.MustExec("use test")
	tk.MustExec("create server prometheus1 foreign data wrapper grpc address=127.0.0.1 port=9091")
	tk.MustExec("create table proms_metric(span_kind char(255), duration int(64), query text) engine=grpc")
	result = tk.MustQuery("select span_kind from proms_metric where query='topk(3, sum by (span_kind) (rate(query_response_time[5m])))'")
	result.Check(testkit.Rows("GET /api5", "GET /api3", "GET /api2"))

	tk.MustExec("drop table if exists root_span")
	tk.MustExec(`create table root_span(timeStamp char(255),
span_id int,
trace_id int,
span_kind char(255),
duration char(255))`)
	tk.MustExec(`insert into root_span values("02:13:13", "750dc35a-3eaa-4d13-bfdb-3a834f05a538","750dc35a-3eaa-4d13-bfdb-3a834f05a538",)`)
	tk.MustExec(`insert into root_span values(2, "GET /api2", "msg 2") ,
(3, "GET /api3", "msg 3"),(4, "GET /api4", "msg 4"),(5, "GET /api5", "msg 5")`)
	result = tk.MustQuery(`Select * from root_span`)
	result.Check(testkit.Rows("1 GET /api1 msg 1",
		"2 GET /api2 msg 2",
		"3 GET /api3 msg 3",
		"4 GET /api4 msg 4",
		"5 GET /api5 msg 5"))

	result = tk.MustQuery(`
SELECT root_span.span_id, root_span.span_kind
From root_span
INNER JOIN
(
   select span_kind
   From proms_metric
   Where query='topk(3, sum by (span_kind) (rate(span_duration[5m])))'
)
proms_metric ON proms_metric.span_kind=root_span.span_kind`)

	//	result = tk.MustQuery(`SELECT tidb_spans.time, tidb_spans.duration, es_logs.msg
	//From tidb_spans
	//INNER JOIN es_logs ON tidb_spans.trace_id=es_logs.id
	//INNER JOIN
	//(
	//   select app_name
	//   From prometheus_metric
	//   Where q='topk(3, sum by (app, proc) (rate(instance_cpu_time_ns[5m])))'
	//)
	//proms_metric ON proms_metric.app_name=tidb_spans.app_name`)
	result.Check(testkit.Rows(
		`2 GET /api2`, `3 GET /api3`, `5 GET /api5`,
	))
}
