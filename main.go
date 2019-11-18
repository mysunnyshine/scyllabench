package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
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

func main() {
	//
	cluster := gocql.NewCluster("10.42.78.194")
	cluster.Keyspace = "demo"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()

	defer session.Close()
	//mset(session)
	mget(session)

}

func mset(session *gocql.Session) {
	var total = int64(0)
	for j := 9000000; j < 9500000; j++ {
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
		time.Sleep(time.Second * 4)
	}
}

func mget(session *gocql.Session) {
	var total = int64(0)
	for j := 1; j <= randNum; j++ {
		query := ""
		args := make([]interface{}, 0)
		for i := 0; i < mgetNum; i++ {
			n := RandInt(0, 50000000)
			fmt.Println("rand n =", n)
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

		startTime := time.Now().UnixNano()
		iter := session.Query(fullQuery, args...).Iter()

		for iter.Scan(&id, &value) {
			fmt.Print("id, value = ", id, value)
		}
		endTime := time.Now().UnixNano()
		deltaTime := (endTime - startTime) / 1000000
		total = total + deltaTime
		s := fmt.Sprintf("MGET 第 %d 次， 时间：%d ms, 平均时间: %d ms", j, deltaTime, total/int64(j))
		fmt.Println(s)
		time.Sleep(time.Second * 5)
	}

}


func RandInt(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	if min >= max {
		return max
	}
	return rand.Intn(max-min) + min
}
