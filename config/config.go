package config

import (
    "encoding/json"
    "os"
    "log"
)

type Configuration struct {
    Mssql   string
    Pgsql   string
    Tables  []string
}

func GetConfig (cfgfile string) (mssql string, pgsql string,tables []string, e error)  {
file, err := os.Open(cfgfile)
    if err != nil {
            log.Println("config:")
            log.Fatal(err)
    }

decoder := json.NewDecoder(file)
configuration := Configuration{}
e = decoder.Decode(&configuration)
mssql = configuration.Mssql 
pgsql = configuration.Pgsql
tables = configuration.Tables 
return mssql,pgsql,tables,e
}
