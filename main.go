// тут лежит тестовый код
// менять вам может потребоваться только коннект к базе
package main

import (
	"database/sql"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
)

// mysql -h my-first-mysql.cw614nsno1om.us-east-2.rds.amazonaws.com --ssl-ca=us-east-2-bundle.pem --ssl-mode=VERIFY_IDENTITY -P 3306 -u admin -p

var (
	// DSN это соединение с базой
	// вы можете изменить этот на тот который вам нужен
	// docker run -p 3306:3306 -v $(PWD):/docker-entrypoint-initdb.d -e MYSQL_ROOT_PASSWORD=1234 -e MYSQL_DATABASE=golang -d mysql
	DSN = "root@tcp(localhost:3306)/golang2017?charset=utf8"
	// DSN = "coursera:5QPbAUufx7@tcp(localhost:3306)/coursera?charset=utf8"
)

func main() {
	// cfg := mysql.Config{
	// 	User:   os.Getenv("DBUSER"),
	// 	Passwd: os.Getenv("DBPASS"),
	// 	Net:    "tcp",
	// 	Addr:   "127.0.0.1:3306",
	// 	DBName: "golang2017",
	// }

	db, err := sql.Open("mysql", DSN)
	if err != nil {
		panic(err)
	}

	err = db.Ping() // вот тут будет первое подключение к базе
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// db.SetMaxOpenConns(20)
	// db.SetMaxIdleConns(20)
	// db.SetConnMaxLifetime(time.Minute * 5)

	handler, err := NewDbExplorer(db)
	if err != nil {
		panic(err)
	}

	fmt.Println("starting server at :8082")
	http.ListenAndServe(":8082", handler)
}
