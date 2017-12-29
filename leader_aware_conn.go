package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"time"
	"strings"
)

type Server struct {
	Addresses []string `json:"addresses"`
	Role      string   `json:"role"`
}

type BalancingConn struct {
	connStr string
	driver *boltDriver
	conn Conn
}

func NewBalancingConn(connStr string, driver *boltDriver) (*BalancingConn, error) {

	c := &BalancingConn{}
	c.driver = driver
	c.connStr = connStr
	conn, err := driver.OpenNeo(connStr)
	if err != nil {
		return nil, err
	}

	c.conn = conn
	return c, nil
}

func checkNotALeader(err error) bool {
	if err != nil {
		return strings.Contains(err.Error(), "Neo.ClientError.Cluster.NotALeader")
	}

	return false
}

func (c *BalancingConn) getServers() ([]Server, error) {
	statement := "CALL dbms.cluster.routing.getServers();"
	data, _, _, err := c.conn.QueryNeoAll(statement, map[string]interface{}{})
	if err != nil {

	}

	var info []Server
	data2 := data[0][1].([]interface{})
	for _, r := range data2 {
		role := r.(map[string]interface{})
		var server Server
		server.Role = role["role"].(string)
		server.Addresses = make([]string, len(role["addresses"].([]interface{})))
		for i, addr := range role["addresses"].([]interface{}) {
			server.Addresses[i] = addr.(string)
		}
		info = append(info, server)
	}

	return info, nil
}

func (c *BalancingConn) PrepareNeo(query string) (Stmt, error) {
	return c.conn.PrepareNeo(query)
}

func (c *BalancingConn) PreparePipeline(query ...string) (PipelineStmt, error) {
	return c.conn.PreparePipeline(query...)
}

func (c *BalancingConn) QueryNeo(query string, params map[string]interface{}) (Rows, error) {
	return c.conn.QueryNeo(query, params)
}

func (c *BalancingConn) QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error) {
	return c.conn.QueryNeoAll(query, params)
}

func (c *BalancingConn) QueryPipeline(query []string, params ...map[string]interface{}) (PipelineRows, error) {
	return c.conn.QueryPipeline(query, params...)
}

func (c *BalancingConn) ExecNeo(query string, params map[string]interface{}) (Result, error) {
	return c.conn.ExecNeo(query, params)
}

func (c *BalancingConn) ExecPipeline(query []string, params ...map[string]interface{}) ([]Result, error) {
	return c.conn.ExecPipeline(query, params...)
}

func (c *BalancingConn) Close() error {
	return c.conn.Close()
}

func (c *BalancingConn) Begin() (driver.Tx, error) {
	return c.conn.Begin()
}

func (c *BalancingConn) SetChunkSize(size uint16) {
	c.conn.SetChunkSize(size)
}

func (c *BalancingConn) SetTimeout(timeout time.Duration) {
	c.conn.SetTimeout(timeout)
}

