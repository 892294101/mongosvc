package main

import (
	"fmt"
	"github.com/892294101/mongosvc/src/svc"
	_ "github.com/go-sql-driver/mysql"
	"net/http"
	"os"
)

func main() {

	go func() {
		_ = http.ListenAndServe("0.0.0.0:29378", nil)
	}()

	m := svc.NewBuildSvc()
	if err := m.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n\n", err)
	}

}
