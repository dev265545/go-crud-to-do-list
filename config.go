package main

import (
	"log"

	"github.com/gocql/gocql"
)

var session *gocql.Session

func init() {
    var err error
    cluster := gocql.NewCluster("127.0.0.1")
    cluster.Keyspace = "todo"
    cluster.Consistency = gocql.Quorum
    session, err = cluster.CreateSession()
    if err != nil {
        log.Fatal("Unable to connect to ScyllaDB: ", err)
    }
}

func GetSession() *gocql.Session {
    return session
}
