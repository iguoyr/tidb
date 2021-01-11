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
	cluster   cluster.Cluster
	store     kv.Storage
	domain    *domain.Domain
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
	tk := testkit.NewTestKit(c, s.store)
	ctx := context.Background()
	var pluginVarNames []string
	pluginName := "file"
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
	tk.MustExec("drop table if exists people")
	tk.MustExec("drop table if exists city")
	tk.MustExec("create table people(city int, name char(255)) ENGINE = file")
	tk.MustExec("insert into people values(1, 'lfkdsk')")
	tk.MustExec("insert into people values(2, 'wph95')")
	tk.MustExec("insert into people values(2, 'guest')")
	result := tk.MustQuery("select * from people")
	result.Check(testkit.Rows("1 lfkdsk", "2 wph95", "2 guest"))
	result = tk.MustQuery("select * from people where city = 2")
	result.Check(testkit.Rows("2 wph95", "2 guest"))
	tk.MustExec("create table city(id int, cityName char(255))")
	tk.MustExec("insert into city values(1, 'beijing')")
	tk.MustExec("insert into city values(2, 'shanghai')")
	result = tk.MustQuery("SELECT city.cityName,people.name FROM city INNER JOIN people ON city.id=people.city")
	result.Check(testkit.Rows("beijing lfkdsk", "shanghai wph95", "shanghai guest"))
}
