all: mssql_pg

mssql_pg: ${GOPATH}/src/github.com/ckuroki/mssql_pg/mssql_pg.go  config/config.go
	go build github.com/ckuroki/mssql_pg

