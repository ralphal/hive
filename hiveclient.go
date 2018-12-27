package hive

import (
	"errors"
	"log"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	thrifthive "github.com/ralphal/hive/thriftlib"
)

// Connection ...
type Connection struct {
	Server string
	Client *thrifthive.TCLIServiceClient

	SessionHandle   *thrifthive.TSessionHandle
	OperationHandle *thrifthive.TOperationHandle
}

var (
	pool  chan *Connection
	_user     string
	_password string

	_batchsize int64 = 1000
	_sleep = 1
)

// SetUserPassword ...
func SetUserPassword(user, password string) {
	_user = user
	_password = password
}

//MakePool create connection pool, initialize connections
func MakePool(server string) {

	pool = make(chan *Connection, 100)

	for i := 0; i < 100; i++ {
		pool <- &Connection{Server: server}
	}

}

//GetHiveConn main entry point for checking out a connection from a list
func GetHiveConn() (conn *Connection, err error) {
	return getConnFromPool()
}

func getConnFromPool() (conn *Connection, err error) {
	conn = <-pool
	if conn.Client == nil || conn.Client.Transport.IsOpen() == false {

		err = conn.Open()
		log.Println(" in create conn, how is client? ", conn.Client, " is err? ", err)
		return conn, err
	}
	return
}

//Open opens a hive connection
func (conn *Connection) Open() error {

	log.Println("creating new hive connection ")

	trans, _ := thrift.NewTSocket(conn.Server)
	trans.Open()

	protocolfac := thrift.NewTBinaryProtocolFactoryDefault()

	conn.Client = thrifthive.NewTCLIServiceClientFactory(trans, protocolfac)

	sreq := thrifthive.NewTOpenSessionReq()
	sreq.ClientProtocol = 6
	sreq.Username = &_user
	sreq.Password = &_password

	r, err := conn.Client.OpenSession(sreq)
	if err != nil {
		log.Println("err=", err)
		return err
	}

	log.Println("rsp=", r)
	conn.SessionHandle = r.SessionHandle

	return nil
}

// Checkin ...
func (conn *Connection) Checkin() {
	pool <- conn
}

// Exec ...
func (conn *Connection) Exec(query string, async bool) error {
	req := thrifthive.NewTExecuteStatementReq()
	req.SessionHandle = conn.SessionHandle
	req.Statement = query
	req.RunAsync = async

	resp, err := conn.Client.ExecuteStatement(req)
	if err != nil {
		return err
	}

	if !isOK(resp.Status) {
		return errors.New("error from server:" + resp.Status.String())
	}

	conn.OperationHandle = resp.OperationHandle

	return nil
}

// GetMetaData ...
func (conn *Connection) GetMetaData() (*thrifthive.TTableSchema, error) {
	req := thrifthive.NewTGetResultSetMetadataReq()
	req.OperationHandle = conn.OperationHandle

	resp, err := conn.Client.GetResultSetMetadata(req)

	if err != nil {
		return nil, err
	}

	schema := resp.GetSchema()

	return schema, nil
}

func isOK(p *thrifthive.TStatus) bool {
	status := p.GetStatusCode()
	return status == thrifthive.TStatusCode_SUCCESS_STATUS || status == thrifthive.TStatusCode_SUCCESS_WITH_INFO_STATUS
}

// FetchRows ...
func (conn *Connection) FetchRows() (*thrifthive.TRowSet, bool, error) {
	req := thrifthive.NewTFetchResultsReq()
	req.OperationHandle = conn.OperationHandle
	req.Orientation = thrifthive.TFetchOrientation_FETCH_NEXT
	req.MaxRows = _batchsize

	socket := conn.Client.Transport.(*thrift.TSocket)
	socket.SetTimeout(10 * time.Second)

	resp, err := conn.Client.FetchResults(req)
	if err != nil {
		return nil, false, err
	}

	if !isOK(resp.Status) {
		return nil, false, errors.New("fetchResults failed, " + resp.Status.String())
	}

	return resp.Results, *resp.HasMoreRows, nil
}

// Wait ...
func (conn *Connection) Wait() (bool, error) {
	for {
		status, state, err := conn.GetStatus()
		if err != nil {
			return false, err
		}

		if state == thrifthive.TOperationState_RUNNING_STATE || status.StatusCode == thrifthive.TStatusCode_STILL_EXECUTING_STATUS {
			time.Sleep(time.Duration(_sleep) * time.Second)
			continue
		}

		if !isOK(status) {
			return false, nil
		}

		break
	}

	return true, nil
}

// GetStatus ...
func (conn *Connection) GetStatus() (*thrifthive.TStatus, thrifthive.TOperationState, error) {
	req := thrifthive.NewTGetOperationStatusReq()
	req.OperationHandle = conn.OperationHandle

	resp, err := conn.Client.GetOperationStatus(req)
	if err != nil {
		return nil, 0, err
	}

	return resp.Status, *resp.OperationState, nil
}