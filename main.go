package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "github.com/gocql/gocql"

/* Before you execute the program, Launch `cqlsh` and execute:
create keyspace demo with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
create table demo.kv(id int, value text, PRIMARY KEY(id));
*/

var msetNum = 200
var mgetNum = 2000
var randNum = 2000000
var goNums = 20
func main() {
	//
	cluster := gocql.NewCluster("10.42.68.17", "10.42.165.220", "10.42.169.144")
	cluster.Keyspace = "demo"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	sessions := make([]*gocql.Session, 0)
/*	for i := 0; i < goNums; i++{
		session1, e1 := cluster.CreateSession()
		if e1 != nil {
			fmt.Println("err", e1)
		}
		fmt.Println("session = ", session1)
		sessions = append(sessions, session1)
	}*/
	defer func() {
		session.Close()
		for _, s := range sessions{
			s.Close()
		}
	}()
	set(session)
	//get(session)
	//mset(session)
	//mget(sessions)

}

func set(session *gocql.Session) {
	var total = int64(0)
	for j := 150000000; j <= 150500000; j++ {
		query := ""
		args := make([]interface{}, 0)
		query = query + "INSERT INTO kv (id, value) VALUES (?, ?);"
		args = append(args, j)
		args = append(args, strconv.Itoa(j))
		startTime := time.Now().UnixNano()
		if err := session.Query(query, args...).Exec(); err != nil {
			fmt.Println("err occured", err)
		}
		endTime := time.Now().UnixNano()
		deltaTime := (endTime - startTime) / 1000000
		total = total + deltaTime
		s := fmt.Sprintf("SET 第 %d 次， 时间：%d ms, 平均时间: %d ms", j, deltaTime, total/int64(j+1-150000000))
		fmt.Println(s)
	}
}



func mset(session *gocql.Session) {
	var total = int64(0)
	for j := 0; j < 250000; j++ {
		begin := "BEGIN BATCH"
		end := "APPLY BATCH"
		query := ""
		args := make([]interface{}, 0)
		for i := 0; i < msetNum; i++ {
			query = query + "INSERT INTO kv (id, value) VALUES (?, ?);"
			args = append(args, msetNum*j+i)
			args = append(args, strconv.Itoa(msetNum*j+i))
		}

		fullQuery := strings.Join([]string{begin, query, end}, "\n")
		startTime := time.Now().UnixNano()
		if err := session.Query(fullQuery, args...).Exec(); err != nil {
			fmt.Println("err occured", err)
		}
		endTime := time.Now().UnixNano()
		deltaTime := (endTime - startTime) / 1000000
		total = total + deltaTime
		s := fmt.Sprintf("MSET 第 %d 次， 时间：%d ms, 平均时间: %d ms", j, deltaTime, total/int64(j+1-9000000))
		fmt.Println(s)
	}
}

func get(session *gocql.Session) {
	var total = int64(0)
	for j := 0; j < 1000000; j++ {
		n := RandInt(150000000, 150500000)
		args := make([]interface{}, 0)
		args = append(args, n)
		fmt.Println("n", n)
		qs := fmt.Sprintf("SELECT id, value FROM demo.kv WHERE id = ?")
		var id int
		var value string
		startTime := time.Now().UnixNano()
		if err := session.Query(qs, args...).Scan(&id, &value); err != nil{
			fmt.Println("## err happened ##", err)
		}
		endTime := time.Now().UnixNano()
		deltaTime := (endTime - startTime) / 1000000
		total = total + deltaTime
		s := fmt.Sprintf("GET 第 %d 次， 时间：%d ms, 平均时间: %d ms", j, deltaTime, total/int64(j + 1))
		fmt.Println(s)
	}

}


func RandInt(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
}

func batchGet(session *gocql.Session, group *sync.WaitGroup, ) {
	defer group.Done()
	query := ""
	args := make([]interface{}, 0)
	for i := 0; i < mgetNum/goNums; i++ {
		n := RandInt(0, 50000000)
		var qs = ""
		if i == 0 {
			qs = fmt.Sprintf("SELECT id, value FROM demo.kv WHERE id in (?")
		} else {
			qs = ",?"
		}
		query = query + qs
		args = append(args, n)
	}
	fullQuery := query + ");"

	var id int
	var value string

	iter := session.Query(fullQuery, args...).Iter()
	count := 0

	for iter.Scan(&id, &value) {
		count = count + 1
	}
	fmt.Println("count = ", count)
}