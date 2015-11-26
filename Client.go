package main  

import (
    "fmt"
    "sort"
    "encoding/json"
    "hash/crc32"
    "io/ioutil"
    "net/http"
)

func main() {
    circle := NewConsistentHash()
    circle.AddNode(NewNode(0, "127.0.0.1:3000"))
    circle.AddNode(NewNode(1, "127.0.0.1:3001"))
    circle.AddNode(NewNode(2, "127.0.0.1:3002"))

    fmt.Println("\nPUT Requests:")
    PutKey(circle,"1","A")
    PutKey(circle,"2","B")
    PutKey(circle,"3","C")
    PutKey(circle,"4","D")
    PutKey(circle,"5","E")
    PutKey(circle,"6","F")
    PutKey(circle,"7","G")
    PutKey(circle,"8","G")
    PutKey(circle,"9","I")
    PutKey(circle,"10","J")

    fmt.Println("\nGET Requests:")
    GetKey("1",circle)
    GetKey("2",circle)
    GetKey("3",circle)
    GetKey("4",circle)
    GetKey("5",circle)
    GetKey("6",circle)
    GetKey("7",circle)
    GetKey("8",circle)
    GetKey("9",circle)
    GetKey("10",circle)

    fmt.Println("\nKey-Value pairs in instance 1 are as follow:\n")
    GetAll("http://127.0.0.1:3000/keys")
    fmt.Println("\nKey-Value pairs in intance 2 are as follow:\n")
    GetAll("http://127.0.0.1:3001/keys")
    fmt.Println("\nKey-Value pairs in instance 3 are as follow:\n")
    GetAll("http://127.0.0.1:3002/keys")
}

type KeyValue struct{
    Key int `json:"key"`
    Value string `json:"value"`
}

type HashCircle []uint32

func (hg HashCircle) Len() int {  
    return len(hg)  
}  
  
func (hg HashCircle) Less(i, j int) bool {  
    return hg[i] < hg[j]  
}  
  
func (hg HashCircle) Swap(i, j int) {  
    hg[i], hg[j] = hg[j], hg[i]  
}  
  
type Node struct {  
    Id       int  
    IP       string    
}  
  
func NewNode(id int, ip string) *Node {  
    return &Node{  
        Id:       id,  
        IP:       ip,  
    }  
}  
  
type ConsistentHash struct {  
    Nodes       map[uint32]Node  
    IsPresent   map[int]bool  
    Circle      HashCircle  
    
}  
  
func NewConsistentHash() *ConsistentHash {  
    return &ConsistentHash{  
        Nodes:     make(map[uint32]Node),   
        IsPresent: make(map[int]bool),  
        Circle:      HashCircle{},  
    }  
}  
  
func (hg *ConsistentHash) AddNode(node *Node) bool {  
 
    if _, ok := hg.IsPresent[node.Id]; ok {  
        return false  
    }  
    str := hg.ReturnNodeIP(node)  
    hg.Nodes[hg.GetHashValue(str)] = *(node)
    hg.IsPresent[node.Id] = true  
    hg.SortHashCircle()  
    return true  
}  
  
func (hg *ConsistentHash) SortHashCircle() {  
    hg.Circle = HashCircle{}  
    for k := range hg.Nodes {  
        hg.Circle = append(hg.Circle, k)  
    }  
    sort.Sort(hg.Circle)  
}  
  
func (hg *ConsistentHash) ReturnNodeIP(node *Node) string {  
    return node.IP 
}  
  
func (hg *ConsistentHash) GetHashValue(key string) uint32 {  
    return crc32.ChecksumIEEE([]byte(key))  
}  
  
func (hg *ConsistentHash) Get(key string) Node {  
    hash := hg.GetHashValue(key)  
    i := hg.Search_Node(hash)  
    return hg.Nodes[hg.Circle[i]]  
}  

func (hg *ConsistentHash) Search_Node(hash uint32) int {  
    i := sort.Search(len(hg.Circle), func(i int) bool {return hg.Circle[i] >= hash })  
    if i < len(hg.Circle) {  
        if i == len(hg.Circle)-1 {  
            return 0  
        } else {  
            return i  
        }  
    } else {  
        return len(hg.Circle) - 1  
    }  
}  
  
func PutKey(circle *ConsistentHash, str string, input string){
        ipAddress := circle.Get(str)  
        address := "http://"+ipAddress.IP+"/keys/"+str+"/"+input
		fmt.Println(address)
        req,err := http.NewRequest("PUT",address,nil)
        client := &http.Client{}
        resp, err := client.Do(req)
        if err!=nil{
            fmt.Println("Error:",err)
        }else{
            defer resp.Body.Close()
            fmt.Println("200 OK")
        }  
}  

func GetKey(key string,circle *ConsistentHash){
    var out KeyValue 
    ipAddress:= circle.Get(key)
	address := "http://"+ipAddress.IP+"/keys/"+key
	fmt.Println(address)
    response,err:= http.Get(address)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}

func GetAll(address string){
     
    var out []KeyValue
    response,err:= http.Get(address)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}