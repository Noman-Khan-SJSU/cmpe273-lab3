package main

import  (
	"strings"
	"sort"
	"net/http"
	"strconv"
	"encoding/json"
	"github.com/julienschmidt/httprouter"
	"fmt"
)

func main(){
	index1 = 0
	index2 = 0
	index3 = 0
	router := httprouter.New()
	router.GET("/keys",retrieveAllKeys)
	router.GET("/keys/:key_id",GetKey)
	router.PUT("/keys/:key_id/:value",PutKeys)
	go http.ListenAndServe(":3000",router)
	go http.ListenAndServe(":3001",router)
	go http.ListenAndServe(":3002",router)
	select {}
}

type KeyValue struct{
	Key int	`json:"key,omitempty"`
	Value string	`json:"value,omitempty"`
} 

var shd1,shd2,shd3 [] KeyValue
var index1,index2,index3 int
type UsingKey []KeyValue
func (a UsingKey) Len() int           { return len(a) }
func (a UsingKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a UsingKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func retrieveAllKeys(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	port := strings.Split(request.Host,":")
	if(port[1]=="3000"){
		sort.Sort(UsingKey(shd1))
		result,_:= json.Marshal(shd1)
		fmt.Fprintln(rw,string(result))
	}else if(port[1]=="3001"){
		sort.Sort(UsingKey(shd2))
		result,_:= json.Marshal(shd2)
		fmt.Fprintln(rw,string(result))
	}else{
		sort.Sort(UsingKey(shd3))
		result,_:= json.Marshal(shd3)
		fmt.Fprintln(rw,string(result))
	}
}

func GetKey(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	out := shd1
	ind := index1
	port := strings.Split(request.Host,":")
	if(port[1]=="3001"){
		out = shd2
		ind = index2
	}else if(port[1]=="3002"){
		out = shd3
		ind = index3
	}
	key,_ := strconv.Atoi(p.ByName("key_id"))
	for i:=0 ; i< ind ;i++{
		if(out[i].Key==key){
			result,_:= json.Marshal(out[i])
			fmt.Fprintln(rw,string(result))
		}
	}
}

func PutKeys(rw http.ResponseWriter, request *http.Request,p httprouter.Params){
	port := strings.Split(request.Host,":")
	key,_ := strconv.Atoi(p.ByName("key_id"))
	if(port[1]=="3000"){
		shd1 = append(shd1,KeyValue{key,p.ByName("value")})
		index1++
	}else if(port[1]=="3001"){
		shd2 = append(shd2,KeyValue{key,p.ByName("value")})
		index2++
	}else{
		shd3 = append(shd3,KeyValue{key,p.ByName("value")})
		index3++
	}
}