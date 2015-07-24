package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"strconv"
	"strings"
)

var serverInfo1 ServerConfig
var done chan *rpc.Call

type ServerConfig struct {
	ServerID                   string
	Protocol                   string
	IpAddress                  string
	Port                       int
	PersistentStorageContainer string
	Methods                    []string
	MyPort                     int
	ChordExist                 bool
	MyIP                       string
}

type JsonReply struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

type Response struct {
	Reply  JsonReply
	client *rpc.Client
}

type Request struct {
	Method string        `json:"method"`
	Params []interface{} `json:"params"`
}
type Triplet struct {
	Key          string
	Relationship string
	Value        TripletValue
}
type TripletValue struct {
	Content    interface{} `json:"content"`
	Size       string      `json:"size"`
	Created    string      `json:"created"`
	Modified   string      `json:"modified"`
	Accessed   string      `json:"accessed"`
	Permission string      `json:"permission"`
}
type UserRequest struct {
	Method     string        `json:"method"`
	Params     []interface{} `json:"params"`
	Permission string        `json:"permission"`
}

type KeyRelation struct {
	Key          string
	Relationship string
}

//Wrapper method to make rpc with Go()
func rpc_Go(method string, args Triplet, resp *Response, ip string, port int, cs chan *rpc.Call) interface{} {
	tempClient, err := jsonrpc.Dial(serverInfo1.Protocol, ip+":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
	}
	(*resp).client = tempClient
	tempClient.Go("DICT3."+method, args, resp, cs)
	return nil
}

func main() {
	b, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	//Configuration file object
	err = json.Unmarshal(b, &serverInfo1)
	if err != nil {
		fmt.Println(err)
	}

	done := make(chan *rpc.Call, 10)

	go func() {
		for {
			reader := bufio.NewReader(os.Stdin)
			req, _ := reader.ReadString('\n')
			var request UserRequest
			var resp Response
			err := json.Unmarshal([]byte(strings.TrimRight(req, "\n")), &request)
			if err != nil {
				fmt.Println(err)
			}
			var triplet Triplet
			triplet = Triplet{}

			if len(request.Params) == 1 {
				triplet.Key = request.Params[0].(string)
			} else if len(request.Params) == 2 {
				triplet.Key = request.Params[0].(string)
				triplet.Relationship = request.Params[1].(string)
			} else if len(request.Params) == 3 {
				triplet.Key = request.Params[0].(string)
				triplet.Relationship = request.Params[1].(string)
				triplet.Value.Content = request.Params[2]
				triplet.Value.Permission = request.Permission
			}
			rpc_Go(request.Method, triplet, &resp, serverInfo1.IpAddress, serverInfo1.Port, done)
		}
	}()
	//Main thread to wait for async response.

	for resp := range done {
		var test *Response
		test = resp.Reply.(*Response)
		if (&test.Reply) != nil {
			j, err := json.Marshal(test.Reply.Result)
			fmt.Println("Result: ",string(j))
			fmt.Println("Error: ",err)
		}
		var tempClient *rpc.Client
		tempClient = test.client
		(*tempClient).Close()

	}

}
