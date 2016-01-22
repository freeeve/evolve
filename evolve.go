package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func main() {
	db, err := sqlx.Connect("postgres", os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	appName := os.Args[1]
	version := getVersion(db, appName)
	log.Println("current schema version:", version)

	latest := getLatest()
	if version < latest {
		log.Printf("updating %s database to: %s\n", appName, latest)
		evolve(db, version, appName)
	} else {
		log.Printf("%s database up to date.\n", appName)
	}
}

func evolve(db *sqlx.DB, startVersion string, appName string) {
	files, _ := ioutil.ReadDir("./schema")
	sort.Sort(ByName(files))
	i := 0
	if startVersion != "-1" {
		for ; i < len(files); i++ {
			if stripExt(files[i].Name()) == startVersion {
				i++
				break
			}
		}
		files = files[i:]
	}
	for _, file := range files {
		log.Println("running file:", "./schema/"+file.Name())
		commands, err := ioutil.ReadFile("./schema/" + file.Name())
		if err != nil {
			log.Fatal(err)
		}
		_ = db.MustExec(string(commands))
		if err != nil {
			log.Fatal(err)
		}
		_ = db.MustExec(fmt.Sprintf("update %s_meta set version=$1", appName), stripExt(file.Name()))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func stripExt(file string) string {
	return strings.Replace(file, ".sql", "", 1)
}

func getLatest() string {
	files, _ := ioutil.ReadDir("./schema")
	sort.Sort(ByName(files))
	return stripExt(files[len(files)-1].Name())
}

type ByName []os.FileInfo

func (a ByName) Len() int           { return len(a) }
func (a ByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByName) Less(i, j int) bool { return a[i].Name() < a[j].Name() }

func getVersion(db *sqlx.DB, appName string) string {
	rows, err := db.Query(fmt.Sprintf("select version from %s_meta;", appName))
	if err != nil {
		log.Println(err)
		return "-1"
	}
	defer rows.Close()

	rows.Next()
	var version string
	err = rows.Scan(&version)
	if err != nil {
		log.Println(err)
		return "-1"
	}
	return version
}
