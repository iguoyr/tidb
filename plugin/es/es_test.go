package es

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
	pluginName := "elasticsearch"
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
	tk.MustExec("create table logs(ID int, body text, query text) engine=elasticsearch")
	result = tk.MustQuery("select body->'$.status' as Status from logs where query='!(status:200) OR error'")
	result.Check(testkit.Rows("500", "500", "401"))
	result = tk.MustQuery(`SELECT body->'$.IP' as ip FROM logs where query='!(status:200) OR error'`)
	result.Check(testkit.Rows(`"3.0.0.201"`, `"2.0.0.222"`, `"2.0.0.223"`))

	tk.MustExec("drop table if exists blacklist")
	tk.MustExec("create table blacklist(ip char(255), level int, message char(255))")
	tk.MustExec(`insert into blacklist values("2.0.0.220", 1, "shanghai.0")`)
	tk.MustExec(`insert into blacklist values("2.0.0.222", 3, "ip: 2.0.0.222 此 ip 高危 ") ,
("2.0.0.223", 3, "2.0.0.223: 此 ip 高危 "),
("1.0.0.220", 2, "beijing.3"),
("4.5.6.7", 2, "")`)
	result = tk.MustQuery(`Select * from blacklist`)
	result.Check(testkit.Rows("2.0.0.220 1 shanghai.0",
		"2.0.0.222 3 ip: 2.0.0.222 此 ip 高危",
		"2.0.0.223 3 2.0.0.223: 此 ip 高危",
		"1.0.0.220 2 beijing.3",
		"4.5.6.7 2 "))

	result = tk.MustQuery(`SELECT blacklist.* FROM blacklist INNER JOIN
(SELECT body->'$.IP' as ip FROM logs where query='!(status:200) OR error')
AS logs ON logs.ip=blacklist.ip`)
	result.Check(testkit.Rows(
		`2.0.0.222 3 ip: 2.0.0.222 此 ip 高危`,
		`2.0.0.223 3 2.0.0.223: 此 ip 高危`))
}
