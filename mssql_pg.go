// iface - interface MSSQL - PostgreSQL

package main

import (
"database/sql"
"bytes"
"fmt"
"os"
"strconv"
"log"
"strings"
"github.com/ckuroki/mssql_pg/config"
"github.com/elgs/gosqljson"
_ "github.com/lib/pq"
_ "bitbucket.org/miquella/mgodbc"
 
)

func procTable(dbdst *sql.DB,dbsrc *sql.DB, schema string, table string) {
var exists uint
var prefix4 string
var sqlbuf bytes.Buffer // create
var sqlbuf2 bytes.Buffer // select
var sqlbuf3 bytes.Buffer // insert
var sqlbuf4 bytes.Buffer // create as select
var auxbuf bytes.Buffer // bind parameters for insert

// Data types map between Mssql -> PostgreSQL

conv := map[string]string {
 "bigint": "bigint",
 "bit": "boolean",
 "char": "varchar",
 "datetime": "timestamp",
 "smalldatetime": "varchar",
 "decimal": "numeric",
 "float": "numeric",
 "image": "bytea",
 "int": "integer",
 "money": "numeric",
 "numeric": "numeric",
 "nvarchar": "varchar",
 "smallint": "smallint",
 "text": "text",
 "tinyint": "smallint",
 "uniqueidentifier": "varchar",
 "varbinary": "bytea",
 "varchar": "varchar",
} 

fmt.Println("Processing "+table)
theCase := "lower"

var sqlcol string
sqlcol ="select column_name,data_type,ordinal_position from information_schema.columns where table_schema = '"+schema+"' and table_name = '"+table+"' order by ordinal_position"

_,d,err := gosqljson.QueryDbToArray(dbsrc, theCase, sqlcol)
    if err != nil {
            log.Println("gosqljson:")
            log.Fatal(err)
    }

if len(d)<1  {
log.Println("Error: table "+schema+"."+table+ " doesnt exists on source database.")
return
}
sqlbuf.WriteString("create table "+schema+"_"+table+"_aux (")
sqlbuf2.WriteString("select ")
sqlbuf3.WriteString("insert into "+schema+"_"+table+"_aux (")
sqlbuf4.WriteString(" select ")

for f := range d {
if f!= 0 {
sqlbuf.WriteString(",")
sqlbuf2.WriteString(",")
sqlbuf3.WriteString(",")
sqlbuf4.WriteString(",")
auxbuf.WriteString(",")
}
sqlbuf.WriteString(d[f][0] + " varchar")
if (conv[d[f][1]]=="timestamp"){
sqlbuf2.WriteString("convert(varchar,"+d[f][0]+",120)")
} else { 
  if (d[f][1]=="varchar"){
   sqlbuf2.WriteString(d[f][0])
   } else { 
     sqlbuf2.WriteString("convert(varchar,"+d[f][0]+")")
   }
}
sqlbuf3.WriteString(d[f][0])
auxbuf.WriteString("$"+strconv.Itoa(f+1))
  sqlbuf4.WriteString(" case when length("+d[f][0]+")=0 then null else "+d[f][0]+ "::"+conv[d[f][1]]+" end as "+d[f][0] )

}
sqlbuf.WriteString(")")
sqlbuf2.WriteString(" from "+schema+"."+table)
sqlbuf3.WriteString(") values ("+auxbuf.String()+")")
sqlbuf4.WriteString(" from "+schema+"_"+table+"_aux ")

    tx,err := dbdst.Begin()
    if err != nil {
            log.Println("Transaction")
            log.Fatal(err)
    }

   _,err = tx.Exec("drop table if exists "+schema+"_"+table+"_aux")
    if err != nil {
            log.Println("Drop")
            log.Println(err)
             tx.Rollback()
             return
    }

   _,err = tx.Exec(sqlbuf.String())
    if err != nil {
            log.Println("Create")
            log.Println(err)
             tx.Rollback()
             return
    }


  _,data,err := gosqljson.QueryDbToArray(dbsrc, theCase, sqlbuf2.String())
    if err != nil {
            log.Println("SourceDB")
            log.Println(err)
             tx.Rollback()
             return
    }


   istmt,err := tx.Prepare(sqlbuf3.String())
    if err != nil {
            log.Println("Prepare..")
            log.Println(sqlbuf3.String())
            log.Println(err)
             tx.Rollback()
             return
    }

  for fila := range data {
   new:=make([]interface{},len(data[fila]))
   for i,v := range data[fila] {
   new[i]= interface{}(v)
   }
 
   _,err = istmt.Exec(new...)
    if err != nil {
             log.Println(err)
             tx.Rollback()
             return
    } 
  } 
  istmt.Close()

  // Checks if table exists
  err = tx.QueryRow("select count(*) as cant from pg_tables where schemaname = 'public' and tablename = '"+strings.ToLower(schema)+"_"+strings.ToLower(table)+"'").Scan(&exists)
   if err != nil {
	log.Fatal(err)
        tx.Rollback()
        return
  }
  if (exists == 0) {
   prefix4=("create table "+schema+"_"+table+" as ")
  } else {
   _,err = tx.Exec("truncate table "+schema+"_"+table)
    if err != nil {
            log.Println("Trunc")
            log.Println(err)
             tx.Rollback()
             return
    }
   prefix4=("insert into "+schema+"_"+table)
  }

   _,err = tx.Exec(prefix4 + sqlbuf4.String())
    if err != nil {
            log.Println("Create as select")
            log.Println(err)
             tx.Rollback()
             return
    }
    tx.Commit()
}

func main() { 
var cfgfile string

 if len(os.Args) == 1 {
  cfgfile="/usr/local/etc/odbc_pg.json"
  fmt.Println("Using default configuration : "+cfgfile)
 } else {
  if len(os.Args) < 3 {
  cfgfile=os.Args[1]
  } else {
   fmt.Fprintf(os.Stderr, "usage: odbc_pg [config.json]\n")
   os.Exit(1)
  }
 }
 mssql,pgsql,tablelist,_ := config.GetConfig(cfgfile)

// Open PostgreSQL db
	pgdb, err := sql.Open("postgres", pgsql)
	if err != nil {
		log.Fatal(err)
	}
	defer pgdb.Close()

// Open Source ODBC db
	msdb, err := sql.Open("mgodbc", mssql)
	if err != nil {
		fmt.Println(err)
                return
	}
	defer msdb.Close()

for _,v := range tablelist {
t:=strings.Split(v,".")
schema:=t[0]
table:=t[1]
procTable(pgdb,msdb,strings.ToLower(schema),strings.ToLower(table))
}

}
