package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)
import "github.com/gocql/gocql"

/* Before you execute the program, Launch `cqlsh` and execute:
create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
create table example.large(id int, PRIMARY KEY(id));
*/

var msetNum = 200
var mgetNum = 2000
var randNum = 20000

func main() {
	//
	cluster := gocql.NewCluster("10.42.78.194", "10.42.29.220", "10.42.76.189")
	cluster.Keyspace = "example"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()

	defer session.Close()
	mset(session)
	//mget(session)

}

func mset(session *gocql.Session) {
	var total = int64(0)
	for j := 0; j < 2000000; j++ {
		begin := "BEGIN BATCH"
		end := "APPLY BATCH"
		query := ""
		args := make([]interface{}, 0)
		for i := 0; i < msetNum; i++ {
			query = query + "INSERT INTO large (id) VALUES (?);"
			args = append(args, i*j+i)
		}

		fullQuery := strings.Join([]string{begin, query, end}, "\n")
		startTime := time.Now().UnixNano()
		if err := session.Query(fullQuery, args...).Exec(); err != nil {
			fmt.Println("err occured", err)
		}
		endTime := time.Now().UnixNano()
		deltaTime := (endTime - startTime) / 1000000
		total = total + deltaTime
		s := fmt.Sprintf("MSET 第 %d 次， 时间：%d ms, 平均时间: %d ms", j, deltaTime, total/int64(j+1))
		fmt.Println(s)
	}
}

func mget(session *gocql.Session) {
	var total = int64(0)
	for j := 1; j <= randNum; j++ {
		query := ""
		args := make([]interface{}, 0)
		for i := 0; i < mgetNum; i++ {
			rand.Seed(time.Now().UnixNano())
			n := rand.Intn(randNum)
			var qs = ""
			if i == 0 {
				qs = fmt.Sprintf("SELECT id FROM example.large WHERE id in (?")
			} else {
				qs = ",?"
			}
			query = query + qs
			args = append(args, n)
		}
		query = query + ");"
		fullQuery := strings.Join([]string{"", query, ""}, "\n")
		startTime := time.Now().UnixNano()
		iter := session.Query(fullQuery, args...).Iter()
		var id int
		for iter.Scan(&id) {
		}
		endTime := time.Now().UnixNano()
		deltaTime := (endTime - startTime) / 1000000
		total = total + deltaTime
		s := fmt.Sprintf("MGET 第 %d 次， 时间：%d ms, 平均时间: %d ms", j, deltaTime, total/int64(j))
		fmt.Println(s)
		time.Sleep(time.Second * 1)
	}

}
