package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"github.com/robfig/cron"
	"io/ioutil"
	"math"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var serverInfo1 ServerConfig
var done chan *rpc.Call

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

var dictionaryFile string
var myDictionary map[KeyRelation]TripletValue

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

type Node struct {
	IP   string
	Port int
	ID   int
}

type DICT3 []string

var pendingReq int
var fingerTable [5]Node
var predecessor Node
var mySuccessor Node
var myNode Node
var c *cron.Cron
var mutex, reqCountMutex *sync.Mutex

//Hash function for IP:Port, Key and Rel
func hashIt(s string, bit int) int {
	h := sha1.New()
	h.Write([]byte(s))
	bs := h.Sum(nil)
	hashValue := math.Mod(float64(bs[len(bs)-1]), math.Exp2(float64(bit)))
	return int(hashValue)
}

//Wrapper method to make rpc with Call()
func rpc_call(reqMethod string, reqParam interface{}, ip string, port int) Node {

	tempClient, _ := jsonrpc.Dial(serverInfo1.Protocol, ip+":"+strconv.Itoa(port))
	defer tempClient.Close()
	var resp Node
	err := tempClient.Call("DICT3."+reqMethod, reqParam, &resp)
	if err != nil {
		fmt.Println(err)
		return Node{}
	}
	return resp
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

	pendingReq = 0
	myDictionary = make(map[KeyRelation]TripletValue)
	mutex = &sync.Mutex{}
	reqCountMutex = &sync.Mutex{}

	c = cron.New()
	c.AddFunc("@every 20s", PeriodicFunc)
	c.Start()
	if len(os.Args) != 2 {
		fmt.Println("Usage: ", os.Args[0], "filename")
	}

	b, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	//Configuration file object
	err = json.Unmarshal(b, &serverInfo1)
	if err != nil {
		fmt.Println(err)
	}

	myHash := hashIt(serverInfo1.MyIP+":"+strconv.Itoa(serverInfo1.MyPort), 5)
	myNode = Node{serverInfo1.MyIP, serverInfo1.MyPort, myHash}

	fmt.Println("MyHash: " + strconv.Itoa(myHash))
	go serverFun(serverInfo1)

	if !serverInfo1.ChordExist {
		fingerTable = [5]Node{myNode, myNode, myNode, myNode, myNode}
		mySuccessor = myNode
		predecessor = Node{}
	}

	// Thread to start collecting data for finger print table.
	if serverInfo1.ChordExist {
		mySuccessor = rpc_call("Join", myNode, serverInfo1.IpAddress, serverInfo1.Port)
	}
	PeriodicFunc()
	fmt.Println("mySuccessor: ", mySuccessor)
	fmt.Println("Predecessor: ", predecessor)
	fmt.Println("FingerTable: ", fingerTable)

	//Main thread to wait for async response.
	done := make(chan *rpc.Call, 10)

	for resp := range done {
		var test *Response
		test = resp.Reply.(*Response)
		if &(test.Reply) != nil {
			j, err := json.Marshal(test.Reply.Result)
			fmt.Println(string(j), err)
		}
		var tempClient *rpc.Client
		tempClient = test.client
		(*tempClient).Close()
	}
	c.Stop()

}
func serverFun(serverInfo1 ServerConfig) {
	fmt.Println("Server Function started")
	//server start
	dictionaryFile = serverInfo1.PersistentStorageContainer

	dict3 := new(DICT3)
	rpc.Register(dict3)
	clientAddr, err := net.ResolveTCPAddr(serverInfo1.Protocol, ":"+strconv.Itoa(serverInfo1.MyPort))
	if err != nil {
		fmt.Println("Error1: ", err.Error)
		os.Exit(1)
	}
	listn, err := net.ListenTCP(serverInfo1.Protocol, clientAddr)
	if err != nil {
		fmt.Println("Error2: ", err.Error)
		os.Exit(1)
	}
	ReadFile(dictionaryFile) //load to map

	for {
		connect, err := listn.Accept()
		if err != nil {
			continue
		}
		go func() {
			jsonrpc.ServeConn(connect)
		}()
	}
}

func ReadFile(fileName string) {
	file, _ := os.Open(fileName)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		temp := scanner.Text()
		var triplet Triplet
		err := json.Unmarshal([]byte(temp), &triplet)
		if err != nil {
			fmt.Println("Error loading triplet into map", err)
		}
		keyRel := KeyRelation{triplet.Key, triplet.Relationship}
		myDictionary[keyRel] = triplet.Value

	}
}

func WriteFile() {
	file, err := os.OpenFile(dictionaryFile, os.O_TRUNC|os.O_WRONLY, 3104)
	defer file.Close()
	if err != nil {
		fmt.Println("Unable to open file for write: " + err.Error())
	}
	for value := range myDictionary {
		temp, _ := json.Marshal(value)
		_, err = file.WriteString(string(temp) + "\n")
	}
	return
}

func (t *DICT3) Join(newNode Node, successor *Node) error {
	var nodeSuccessor Node
	t.FindSuccessor(newNode, &nodeSuccessor)
	if nodeSuccessor.ID == myNode.ID && nodeSuccessor.Port == myNode.Port {
		predecessor = newNode
	}
	fmt.Println(nodeSuccessor)
	*successor = nodeSuccessor
	fmt.Println(fingerTable)
	return nil
}

func (t *DICT3) FindSuccessor(newNode Node, successor *Node) error {
	// fmt.Println("Inside findSuccessor")

	hash := newNode.ID
	//To prevent ignoring node with hash 0
	// Condition for first joining node.
	if mySuccessor.ID == myNode.ID && !serverInfo1.ChordExist {
		fmt.Println("I am the successor")
		*successor = myNode
		predecessor = newNode
		fingerTable[0] = newNode
		serverInfo1.ChordExist = true
		return nil
	}
	//Ring egde condition
	if myNode.ID > mySuccessor.ID {
		// Case myNode.ID = 28; muSuccessor.ID = 4 ; hash = 30 or 2
		if hash > myNode.ID || hash <= mySuccessor.ID {
			*successor = mySuccessor
			return nil
		} else if hash == myNode.ID {
			*successor = myNode
			return nil
		}
	}
	if hash > myNode.ID && hash <= fingerTable[0].ID {
		//fmt.Println("My successor is your successor")
		if hash == myNode.ID {
			*successor = myNode
			return nil
		}
		*successor = mySuccessor
		return nil

	} else {
		var closestPre Node
		t.Closest_predecessor(hash, &closestPre)
		if closestPre.ID == myNode.ID {
			*successor = myNode
			return nil
		} else if mySuccessor.ID == myNode.ID && closestPre.ID == myNode.ID {
			// //Case mySuccessor failed and not yet updated.
			// *successor = myNode
			return nil
		}
		*successor = rpc_call("FindSuccessor", newNode, closestPre.IP, closestPre.Port)
		return nil
	}
	return nil
}

// func (t *DICT3)Find
func (t *DICT3) Closest_predecessor(hash int, predecessor *Node) error {
	// fmt.Println("Inside closest predecessor")
	// we need this because finger table entries need not be in increasing order.
	temp := Node{}
	for i := 4; i >= 0; i-- {

		if fingerTable[i].ID <= hash && temp.ID < fingerTable[i].ID {
			temp = fingerTable[i]
		}
	}
	if temp.ID != 0 {
		*predecessor = temp
		return nil
	}
	*predecessor = myNode
	return nil
}
func PeriodicFunc() {
	mutex.Lock()
	fmt.Println("PeriodicFunc Call")
	Stabilize()
	checkPredecessor()
	fix_finger_table()
	Purge()

	//Send relavent keys to predecessor
	if predecessor.Port != 0 {
		fmt.Println("Key clean up")
		//What are the predecessors nodes?
		tempDict := make(map[KeyRelation]TripletValue)
		for kr, _ := range myDictionary {
			kHash := hashIt(kr.Key, 3)
			rHash := hashIt(kr.Relationship, 2)
			krHash, _ := strconv.Atoi(strconv.Itoa(kHash) + strconv.Itoa(rHash))
			krHash = int(math.Mod(float64(krHash), float64(32)))
			if predecessor.ID > krHash {
				tempDict[kr] = myDictionary[kr]
				delete(myDictionary, kr)
			}
		}
		if len(tempDict) != 0 {
			//Making a call to predecessor
			rpc_call("SendKeys", tempDict, predecessor.IP, predecessor.Port)
		}
	}

	fmt.Println("mySuccessor: ", mySuccessor, " Predecessor: ", predecessor, " FingerTable: ", fingerTable)
	mutex.Unlock()

}
func Stabilize() {
	//First node in chord ring
	if mySuccessor.ID == myNode.ID {
		fingerTable[0] = mySuccessor
	}
	defer func() {
		if err := recover(); err != nil {
			mySuccessor = myNode
		}
	}()
	x := rpc_call("FindPredecessor", myNode, mySuccessor.IP, mySuccessor.Port)
	// Successor has a valid predecessor. Make a check if I fall in between them
	if x.ID != 0 && x.Port != 0 {
		if mySuccessor.ID < myNode.ID && (myNode.ID < x.ID || x.ID < mySuccessor.ID) {
			mySuccessor = x
			fingerTable[0] = x
		} else if mySuccessor.ID == myNode.ID {
			//Case successor not updated
			mySuccessor = predecessor
			fingerTable[0] = mySuccessor
		} else if mySuccessor.ID > myNode.ID && (x.ID > myNode.ID && x.ID < mySuccessor.ID) {
			mySuccessor = x
			fingerTable[0] = mySuccessor
		}
	}

	//successor.notify()
	rpc_call("Notify", myNode, mySuccessor.IP, mySuccessor.Port)
}
func (t *DICT3) Notify(newNode Node, resp *Node) error {
	// predecessor is nil
	if predecessor.ID == 0 && predecessor.Port == 0 {
		predecessor = newNode
		return nil
	} else if predecessor.ID == myNode.ID {
		// Predecessor is not updated yet
		predecessor = newNode
	}
	//Ring edge condition
	if predecessor.ID > myNode.ID && (newNode.ID > predecessor.ID || newNode.ID < myNode.ID) {
		// Case pre = 28, myNode = 5 and newNode = 30 or 2
		predecessor = newNode
	} else if newNode.ID > predecessor.ID && newNode.ID < myNode.ID {
		//Case pre = 23, myNode = 28 and newNode = 25
		predecessor = newNode
	}
	return nil
}

//Calculate values for the finger tables
func fix_finger_table() {
	for i := 0; i < 5; i++ {
		var tempNode Node
		tempNode.ID = int(math.Mod(float64(myNode.ID+int(math.Exp2(float64(i)))), float64(32)))
		defer func() {
			if err := recover(); err != nil {
				fmt.Println("Unable to find fingerTable entri" + strconv.Itoa(i))
			}
		}()
		//But what if mySuccessor fails?
		fingerTable[i] = rpc_call("FindSuccessor", tempNode, mySuccessor.IP, mySuccessor.Port)
	}
}

func (t *DICT3) FindPredecessor(args Node, resp *Node) error {
	if predecessor.Port == 0 {
		predecessor = args
	}
	*resp = predecessor
	return nil
}

//Called periodically to check for existence of its oredecessor
//Make a simple rpc and see that no connection error is returned
func checkPredecessor() {
	if predecessor.Port == 0 {
		fmt.Println("Predecessor failed")
		return
	}
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("Predecessor failed")
			if fingerTable[0].ID == predecessor.ID {
				fingerTable[0] = Node{}
				fix_finger_table()
			}
			predecessor = Node{}
		}
	}()
	rpc_call("Ping", nil, predecessor.IP, predecessor.Port)
}

//Simple RPC to check if the node is alive
func (t *DICT3) Ping(args Node, resp *Node) error {
	return nil
}

func forwardReq(method string, args Triplet, resp *Response, channel chan *rpc.Call, targetNode Node) {
	rpc_Go(method, args, resp, targetNode.IP, targetNode.Port, channel)
}

//Wait for response from a forwarded request
func waitForResponse(originalResp *Response, channel chan *rpc.Call) {
	resp := <-channel
	var test *Response
	test = resp.Reply.(*Response)
	test.client.Close()
	originalResp.Reply = test.Reply
	return
}

func (t *DICT3) LookUp(triplet Triplet, resp *Response) error {
	fmt.Println("Look Up for: ", triplet) //First calculate key,relation hashes and check if req should be forwarded or served
	if triplet.Key == "" {
		LookUpKey(triplet, resp)
		return nil
	} else if triplet.Relationship == "" {
		LookUpRelation(triplet, resp)
		return nil
	} else {
		keyHash := hashIt(triplet.Key, 3)
		relHash := hashIt(triplet.Relationship, 2)
		keyRelHash, _ := strconv.Atoi(strconv.Itoa(keyHash) + strconv.Itoa(relHash))
		keyRelHash = int(math.Mod(float64(keyRelHash), float64(32)))

		if (myNode.ID < predecessor.ID && (keyRelHash <= myNode.ID || keyRelHash > predecessor.ID)) || (keyRelHash <= myNode.ID && keyRelHash > predecessor.ID) {
			//Serve the req
			keyRel := KeyRelation{triplet.Key, triplet.Relationship}
			answer, prs := myDictionary[keyRel]
			if !prs {
				resp.Reply.Error = "Key and relation not found \n"
				return nil
			}
			answer.Accessed = string(time.Now().Format(time.RFC850))
			resp.Reply.Result = answer
			myDictionary[keyRel] = answer
		} else {
			//forward the request
			//find the node to which we need to forward the req
			var d DICT3
			var targetNode Node
			d.Closest_predecessor(keyRelHash, &targetNode)
			if targetNode.ID == myNode.ID {
				targetNode = mySuccessor
			}
			var tempResp *Response
			tempResp = &Response{}
			tempChan := make(chan *rpc.Call, 1)
			forwardReq("LookUp", triplet, tempResp, tempChan, targetNode)
			waitForResponse(resp, tempChan)
		}
	}
	return nil
}

func (d *DICT3) Insert(triplet Triplet, resp *Response) error {
	//First calculate key,relation hashes and check if req should be forwarded or served
	keyHash := hashIt(triplet.Key, 3)
	relHash := hashIt(triplet.Relationship, 2)
	keyRelHash, _ := strconv.Atoi(strconv.Itoa(keyHash) + strconv.Itoa(relHash))
	keyRelHash = int(math.Mod(float64(keyRelHash), float64(32)))
	fmt.Println("Insert for: ", triplet, " with hash: ", keyRelHash)
	//KeyRelHash belongs to me
	if (myNode.ID < predecessor.ID && (keyRelHash <= myNode.ID || keyRelHash > predecessor.ID)) || (keyRelHash <= myNode.ID && keyRelHash > predecessor.ID) {
		keyRel := KeyRelation{triplet.Key, triplet.Relationship}
		_, prs := myDictionary[keyRel]
		if prs {
			//Triplet exists
			(*resp).Reply.Result = false
			(*resp).Reply.Error = "Triplet exists"
		} else {
			triplet.Value.Created = string(time.Now().Format(time.RFC850))
			triplet.Value.Accessed = string(time.Now().Format(time.RFC850))
			triplet.Value.Modified = string(time.Now().Format(time.RFC850))
			temp, _ := json.Marshal(triplet.Value.Content)
			triplet.Value.Size = strconv.Itoa(len(temp)) + "B"
			myDictionary[keyRel] = triplet.Value
			(*resp).Reply.Result = true
		}
		fmt.Println((*resp).Reply.Result)
		return nil
	}
	var tempNode, targetNode Node
	tempNode = Node{}
	tempNode.ID = keyRelHash
	d.FindSuccessor(tempNode, &targetNode)

	var tempResp *Response
	tempResp = &Response{}
	tempChan := make(chan *rpc.Call, 1)
	forwardReq("Insert", triplet, tempResp, tempChan, targetNode)
	waitForResponse(resp, tempChan)
	return nil
}

func (d *DICT3) InsertOrUpdate(triplet Triplet, resp *Response) error {
	fmt.Println("InsertOrUpdate for :", triplet)
	keyHash := hashIt(triplet.Key, 3)
	relHash := hashIt(triplet.Relationship, 2)
	keyRelHash, _ := strconv.Atoi(strconv.Itoa(keyHash) + strconv.Itoa(relHash))
	keyRelHash = int(math.Mod(float64(keyRelHash), float64(32)))

	//KeyRelHash belongs to me
	if (myNode.ID < predecessor.ID && (keyRelHash <= myNode.ID || keyRelHash > predecessor.ID)) || (keyRelHash <= myNode.ID && keyRelHash > predecessor.ID) {
		keyRel := KeyRelation{triplet.Key, triplet.Relationship}
		x, prs := myDictionary[keyRel]
		if prs {
			if x.Permission == "RW" {
				triplet.Value.Modified = string(time.Now().Format(time.RFC850))
				triplet.Value.Accessed = string(time.Now().Format(time.RFC850))
				triplet.Value.Created = x.Created
				temp, _ := json.Marshal(triplet.Value.Content)
				triplet.Value.Size = strconv.Itoa(len(temp)) + "B"
				myDictionary[keyRel] = triplet.Value
				resp.Reply.Result = true
				return nil
			}
			resp.Reply.Result = false
			resp.Reply.Error = "No permission to update"

		} else {
			triplet.Value.Created = string(time.Now().Format(time.RFC850))
			triplet.Value.Accessed = string(time.Now().Format(time.RFC850))
			triplet.Value.Modified = string(time.Now().Format(time.RFC850))
			temp, _ := json.Marshal(triplet.Value.Content)
			triplet.Value.Size = strconv.Itoa(len(temp)) + "B"
			myDictionary[keyRel] = triplet.Value

			resp.Reply.Result = true
		}
		return nil
	}
	var tempNode, targetNode Node
	tempNode = Node{}
	tempNode.ID = keyRelHash
	d.FindSuccessor(tempNode, &targetNode)

	var tempResp *Response
	tempResp = &Response{}
	tempChan := make(chan *rpc.Call, 1)
	forwardReq("InsertOrUpdate", triplet, tempResp, tempChan, targetNode)
	waitForResponse(resp, tempChan)
	return nil
}

func (d *DICT3) Delete(triplet Triplet, resp *Response) error {
	fmt.Println("Delete req for triplet: ", triplet)

	keyHash := hashIt(triplet.Key, 3)
	relHash := hashIt(triplet.Relationship, 2)
	keyRelHash, _ := strconv.Atoi(strconv.Itoa(keyHash) + strconv.Itoa(relHash))
	keyRelHash = int(math.Mod(float64(keyRelHash), float64(32)))

	//KeyRelHash belongs to me
	if (myNode.ID < predecessor.ID && (keyRelHash <= myNode.ID || keyRelHash > predecessor.ID)) || (keyRelHash <= myNode.ID && keyRelHash > predecessor.ID) {
		keyRel := KeyRelation{triplet.Key, triplet.Relationship}
		tempVal, prs := myDictionary[keyRel]
		if prs && tempVal.Permission == "RW" {
			delete(myDictionary, keyRel)
		}
		return nil
	}

	var tempNode, targetNode Node
	tempNode = Node{}
	tempNode.ID = keyRelHash
	d.FindSuccessor(tempNode, &targetNode)

	var tempResp *Response
	tempResp = &Response{}
	tempChan := make(chan *rpc.Call, 1)
	forwardReq("Delete", triplet, tempResp, tempChan, targetNode)
	waitForResponse(resp, tempChan)
	return nil
}

func Purge() {
	for kr, val := range myDictionary {
		if val.Permission == "RW" {
			lastAccessTime, _ := time.Parse(time.RFC850, val.Accessed)
			if time.Since(lastAccessTime).Minutes() > float64(3) {
				delete(myDictionary, kr)
			}
		}
		return
	}
}

func (d *DICT3) ListIDs(triplet Triplet, resp *Response) error {
	//No need for any hash computation. Append your results and forward
	//But check if the request came back to you after forwarding. In that case simply return.
	if resp.Reply.Result != nil && len(resp.Reply.Result.([]KeyRelation)) > 0 {
		tempKR := (*resp).Reply.Result.([]KeyRelation)[0]
		_, prs := myDictionary[tempKR]
		if prs {
			//The request traversed the entire ring
			return nil
		}
	}
	for id, _ := range myDictionary {
		if (*resp).Reply.Result == nil {
			(*resp).Reply.Result = []KeyRelation{}
		}
		(*resp).Reply.Result = append((*resp).Reply.Result.([]KeyRelation), id)
	}

	tempChan := make(chan *rpc.Call, 1)
	var tempResp *Response
	forwardReq("ListIDs", triplet, tempResp, tempChan, mySuccessor)
	waitForResponse(tempResp, tempChan)
	for i := 0; i < len((*tempResp).Reply.Result.([]KeyRelation)); i++ {
		(*resp).Reply.Result = append((*resp).Reply.Result.([]KeyRelation), (*tempResp).Reply.Result.([]KeyRelation)[i])
	}
	return nil
}
func LookUpKey(triplet Triplet, resp *Response) error {
	//Compute hash and forward specific requests to specific nodes.
	fmt.Println("List Keys for: ", triplet)
	//First calculate key,relation hashes and check if req should be forwarded or served
	relHash := hashIt(triplet.Relationship, 2)
	uniqKeys := make(map[KeyRelation]TripletValue)
	for i := 0; i < 8; i++ {
		keyRelHash, _ := strconv.Atoi(strconv.Itoa(i) + strconv.Itoa(relHash))
		keyRelHash = int(math.Mod(float64(keyRelHash), float64(32)))
		var d *DICT3
		tempNode := Node{}
		tempNode.ID = keyRelHash
		var targetNode Node
		var tempResp *Response
		tempResp = &Response{}
		d.FindSuccessor(tempNode, &targetNode)

		//Send req to get keyrelations for corresponding keyRelHash
		tempChan := make(chan *rpc.Call, 1)
		tempClient, err := jsonrpc.Dial(serverInfo1.Protocol, targetNode.IP+":"+strconv.Itoa(targetNode.Port))
		if err != nil {
			fmt.Println(err)
		}
		(*tempResp).client = tempClient
		temp := TripletAndHash{triplet, keyRelHash}
		tempClient.Go("DICT3.GetKeyOrRelation", temp, tempResp, tempChan)

		waitForResponse(tempResp, tempChan)

		//Filter the unwanted keys from the array
		if tempResp.Reply.Result != nil {
			tempResult := tempResp.Reply.Result.(map[string]interface{})
			tempKR := KeyRelation{tempResult["Key"].(string), tempResult["Relationship"].(string)}
			tempValMap := tempResult["Value"].(map[string]interface{})
			b,_ :=json.Marshal(tempValMap)
			var tempVal TripletValue
			json.Unmarshal(b,&tempVal)
			uniqKeys[tempKR] = tempVal
		}
	}
	for kr2, val := range uniqKeys {
		if (*resp).Reply.Result == nil {
			(*resp).Reply.Result = []Triplet{}
		}
		(*resp).Reply.Result = append((*resp).Reply.Result.([]Triplet), Triplet{kr2.Key,kr2.Relationship,val} )
	}
	return nil
}

type TripletAndHash struct {
	T Triplet
	H int
}

func LookUpRelation(triplet Triplet, resp *Response) error {
	//Compute hash and forward specific requests to specific nodes.
	fmt.Println("List Relation for: ", triplet)
	//First calculate key,relation hashes and check if req should be forwarded or served
	keyHash := hashIt(triplet.Key, 3)
	uniqRels := make(map[KeyRelation]TripletValue)
	for i := 0; i < 4; i++ {
		keyRelHash, _ := strconv.Atoi(strconv.Itoa(keyHash) + strconv.Itoa(i))
		keyRelHash = int(math.Mod(float64(keyRelHash), float64(32)))
		var d *DICT3
		tempNode := Node{}
		tempNode.ID = keyRelHash
		var targetNode Node
		var tempResp *Response
		tempResp = &Response{}
		d.FindSuccessor(tempNode, &targetNode)

		tempChan := make(chan *rpc.Call, 1)

		//Send req to get keyrelations for corresponding keyRelHash
		tempClient, err := jsonrpc.Dial(serverInfo1.Protocol, targetNode.IP+":"+strconv.Itoa(targetNode.Port))
		if err != nil {
			fmt.Println(err)
		}
		(*tempResp).client = tempClient
		temp := TripletAndHash{triplet, keyRelHash}
		tempClient.Go("DICT3.GetKeyOrRelation", temp, tempResp, tempChan)

		waitForResponse(tempResp, tempChan)

		//Filter the unwanted keys from the array
		if tempResp.Reply.Result != nil {
			tempResult := tempResp.Reply.Result.(map[string]interface{})
			tempKR := KeyRelation{tempResult["Key"].(string), tempResult["Relationship"].(string)}
			tempValMap := tempResult["Value"].(map[string]interface{})
			b,_ :=json.Marshal(tempValMap)
			var tempVal TripletValue
			json.Unmarshal(b,&tempVal)
			uniqRels[tempKR] = tempVal
		}
	}
	for kr2, val := range uniqRels {
		if (*resp).Reply.Result == nil {
			(*resp).Reply.Result = []Triplet{}
		}
		(*resp).Reply.Result = append((*resp).Reply.Result.([]Triplet), Triplet{kr2.Key,kr2.Relationship,val} )
	}
	return nil
}

func (d *DICT3) GetKeyOrRelation(tnH TripletAndHash, resp *Response) error {

	if tnH.T.Key == "" {
		rel := tnH.T.Relationship
		for kr, val := range myDictionary {
			rHash := hashIt(kr.Relationship, 2)
			kHash := hashIt(kr.Key, 3)
			krHash, _ := strconv.Atoi(strconv.Itoa(kHash) + strconv.Itoa(rHash))
			krHash = int(math.Mod(float64(krHash), float64(32)))

			if kr.Relationship == rel && tnH.H == krHash {
				resp.Reply.Result = Triplet{kr.Key,kr.Relationship, val}
			}
		}
	} else {
		key := tnH.T.Key
		for kr, val := range myDictionary {
			rHash := hashIt(kr.Relationship, 2)
			kHash := hashIt(kr.Key, 3)
			krHash, _ := strconv.Atoi(strconv.Itoa(kHash) + strconv.Itoa(rHash))
			krHash = int(math.Mod(float64(krHash), float64(32)))

			if kr.Key == key && tnH.H == krHash {
				resp.Reply.Result = Triplet{kr.Key,kr.Relationship, val}

			}
		}
	}
	return nil
}

func (d *DICT3) SendKeys(args map[KeyRelation]TripletValue, resp *Response) error {
	for kr, val := range args {
		myDictionary[kr] = val
	}
	return nil
}

func (d *DICT3) Shutdown(triple Triplet, resp *Response) error {
	fmt.Println("Shutdown")
	for getPendingReqCount() != 0 {
		fmt.Println("I still have some requests to serve.But I should not accept any new reqs")
		time.Sleep(2 * time.Second)
	}

	rpc_call("SendKeys", myDictionary, mySuccessor.IP, mySuccessor.Port)
	os.Exit(1)

	return nil
}

func incPendingReqCount() {
	reqCountMutex.Lock()
	pendingReq++
	reqCountMutex.Unlock()
}

func decPendingReqCount() {
	reqCountMutex.Lock()
	pendingReq--
	reqCountMutex.Unlock()
}

func getPendingReqCount() int {
	reqCountMutex.Lock()
	count := pendingReq
	reqCountMutex.Unlock()

	return count
}
