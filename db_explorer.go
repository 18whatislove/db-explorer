package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

type (
	Table struct {
		Name    string    `json:"name"`
		Columns []*Column `json:"columns"`
	}

	Column struct {
		N        int    `json:"-"`
		Name     string `json:"name"`
		Type     string `json:"type"`
		IsPK     bool   `json:"pk"`
		NullAble bool   `json:"null_able"`
	}
)

func (t *Table) GetPK() *Column {
	for _, c := range t.Columns {
		if c.IsPK {
			return c
		}
	}
	return nil
}

type DbExplorer struct {
	conn   *sql.DB
	Tables []*Table `json:"tables"`
}

func (e *DbExplorer) GetTable(tableName string) (table *Table) {
	for _, table := range e.Tables {
		if table.Name == tableName {
			return table
		}
	}
	return
}

func (e *DbExplorer) showTables() error {
	tables := make([]*Table, 0)
	query := "SHOW TABLES;"

	rows, err := e.conn.Query(query)
	if err != nil {
		return err
	}

	for rows.Next() {
		var tableName string
		rows.Scan(&tableName)
		tables = append(tables, &Table{Name: tableName})
	}

	if err := rows.Err(); err != nil {
		return err
	}

	rows.Close()
	e.Tables = tables
	return nil
}

func (e *DbExplorer) showFullColumns() error {
	// query := "SHOW FULL COLUMNS FROM ? FROM golang2017;"
	for _, table := range e.Tables {
		query := fmt.Sprintf("SHOW FULL COLUMNS FROM %s;", table.Name)
		rows, err := e.conn.Query(query)
		if err != nil {
			return err
		}
		columnTypes, _ := rows.ColumnTypes()
		data := make([]interface{}, len(columnTypes))
		columnDataset := make([]interface{}, len(columnTypes))

		for i := range columnDataset {
			columnDataset[i] = &data[i]
		}

		cols := make([]*Column, 0)
		n := 0 // line number
		for rows.Next() {
			err := rows.Scan(columnDataset...)
			if err != nil {
				return err
			}

			c := &Column{N: n}
			// Field
			if columnName, ok := data[0].([]byte); ok {
				c.Name = string(columnName)
			}
			// Type
			if columnType, ok := data[1].([]byte); ok {
				switch columnType := string(columnType); columnType {
				case "int":
					c.Type = columnType
				case "varchar(255)", "text", "char":
					c.Type = "string"
					// case "datetime", "time", "timestamp", "year", "data":
					// 	c.Type = "time.Time"
				}
			}
			// if null able
			if value, ok := data[3].([]byte); ok {
				if string(value) == "YES" {
					c.NullAble = true
				}
			}
			// if primary key
			if pk, ok := data[4].([]byte); ok {
				if string(pk) == "PRI" {
					c.IsPK = true
				}
			}
			cols = append(cols, c)
			n++
		}

		if err := rows.Err(); err != nil {
			return err
		}
		table.Columns = cols
		rows.Close()
	}
	return nil
}

func (e *DbExplorer) Insert(table *Table, data map[string]interface{}) (rowID int, err error) {
	dataset := func(params map[string]interface{}) []interface{} {
		dataset := make([]interface{}, len(table.Columns))
		for _, c := range table.Columns {

			if value, exists := params[c.Name]; exists {
				dataset[c.N] = value
			} else if !exists && !c.NullAble {
				switch c.Type {
				case "int", "float64":
					dataset[c.N] = 0
				case "string":
					dataset[c.N] = ""
				case "bool":
					dataset[c.N] = false
				}
			}
		}
		return dataset
	}(data)

	query := fmt.Sprintf(`INSERT INTO %s VALUES(%s);`, table.Name, strings.Repeat("?, ", len(dataset)-1)+"?")
	db := e.conn

	result, err := db.Exec(query, dataset...)
	if err != nil {
		log.Printf("row insert into %s failed: %s\n", table.Name, err)
		return 0, fmt.Errorf("dbexplorer: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		log.Printf("getting last insert id %s failed: %s\n", table.Name, err)
		return 0, fmt.Errorf("dbexplorer: %w", err)
	}
	return int(id), nil
}

func (e *DbExplorer) Post(w http.ResponseWriter, r *http.Request) {
	var data map[string]interface{}

	parsedPath := parsePath(r.URL.Path)

	tableName := parsedPath["table"]
	t := e.GetTable(tableName)
	cols := t.Columns

	// parse and validate
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	// serialization
	err = json.Unmarshal(body, &data)
	if err != nil {
		JsonResponse(w, map[string]string{"error": err.Error()}, http.StatusBadRequest)
		return
	}
	// fmt.Printf("%+v\n", data)
	// validation
	err = ParamsValidation(data, cols, map[string]bool{"ignore": true})
	if err != nil {
		JsonResponse(w, map[string]string{"error": err.Error()}, http.StatusBadRequest)
		return
	}
	// create slice based on map
	fmt.Printf("%#v", data)
	id, err := e.Insert(t, data)
	if err != nil {
		JsonResponse(w, map[string]string{}, http.StatusInternalServerError)
		return
	}
	JsonResponse(w, map[string]map[string]int{"response": {t.GetPK().Name: id}}, http.StatusOK)
}

func (e *DbExplorer) Drop(table *Table, pk int) int {
	db := e.conn
	column := table.GetPK()
	if column == nil {
		log.Println("pk does not exist")
		return 0
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?;", table.Name, column.Name)
	result, err := db.Exec(query, pk)
	if err != nil {
		log.Println("exec error:", err)
		return 0
	}
	deletedRows, err := result.RowsAffected()
	if err != nil {
		// method RowsAffected is not compatible with db driver
		log.Println("rows affected error:", err)
		return 0
	}
	return int(deletedRows)
}

func (e *DbExplorer) Delete(w http.ResponseWriter, r *http.Request) {
	var deleted int
	// parse table and id
	parsed := parsePath(r.URL.Path)
	tableName, exists := parsed["table"]
	if exists {
		id, exists := parsed["id"]
		if exists {
			t := e.GetTable(tableName)
			id, _ := strconv.Atoi(id)
			deleted = e.Drop(t, id)
		}
	}
	JsonResponse(w, map[string]map[string]int{"response": {"deleted": deleted}}, http.StatusOK)
}

func (e *DbExplorer) Update(table *Table, id string, params map[string]interface{}) int {
	db := e.conn
	pairs := make([]string, 0)

	var t string // template name=value
	var a []interface{}

	for name, value := range params {
		switch value.(type) {
		case int:
			t = "%s = %d"
			a = []interface{}{name, value}
		case string:
			t = "%s = %q"
			a = []interface{}{name, value}
		case float64:
			t = "%s = %f"
			a = []interface{}{name, value}
		case nil:
			t = "%s = null"
			a = []interface{}{name}
		}
		pairs = append(pairs, fmt.Sprintf(t, a...))
	}
	column := table.GetPK()
	query := fmt.Sprintf(`UPDATE %s SET %s WHERE %s = ?;`, table.Name, strings.Join(pairs, ", "), column.Name)

	result, err := db.Exec(query, id)
	if err != nil {
		log.Println("UPDATE:", err, query, id)
		return 0
	}
	updated, _ := result.RowsAffected()
	return int(updated)
}

func (e *DbExplorer) Put(w http.ResponseWriter, r *http.Request) {
	var data map[string]interface{}
	parsedPath := parsePath(r.URL.Path)

	tableName := parsedPath["table"]
	t := e.GetTable(tableName)
	cols := t.Columns

	// parse and validate
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	// serialization
	err = json.Unmarshal(body, &data)
	if err != nil {
		JsonResponse(w, map[string]string{"error": err.Error()}, http.StatusBadRequest)
		return
	}
	// fmt.Printf("%+v\n", data)
	// validation
	err = ParamsValidation(data, cols, map[string]bool{})
	if err != nil {
		JsonResponse(w, map[string]string{"error": err.Error()}, http.StatusBadRequest)
		return
	}
	id := parsedPath["id"]
	// strconv.Atoi(id)
	updated := e.Update(t, id, data)

	// response
	JsonResponse(w, map[string]map[string]int{"response": {"updated": updated}}, http.StatusOK)
}

func NewDbExplorer(conn *sql.DB) (http.Handler, error) {
	var err error
	e := &DbExplorer{conn: conn}
	err = e.showTables()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	err = e.showFullColumns()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return e, nil
}

func (e *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		p, m string = r.URL.Path, r.Method
	)

	// table names
	names := func() (names []string) {
		names = make([]string, 0, len(e.Tables))
		for _, table := range e.Tables {
			names = append(names, table.Name)
		}
		return
	}()
	p1 := fmt.Sprintf(`^/(?:%s)$`, strings.Join(names, "|"))     // get
	p2 := fmt.Sprintf(`^/(?:%s)/$`, strings.Join(names, "|"))    // put
	p3 := fmt.Sprintf(`^/(?:%s)/\d+$`, strings.Join(names, "|")) // get delete post

	switch {
	case regexp.MustCompile(`^/$`).MatchString(p):
		switch m {
		case http.MethodGet:
			GetTableNames(e)(w, r)
		default:
			JsonResponse(w, map[string]string{}, http.StatusMethodNotAllowed)
		}
	case regexp.MustCompile(p1).MatchString(p):
		switch m {
		case http.MethodGet:
			GetTableRecords := func(w http.ResponseWriter, r *http.Request) {
				var (
					err           error
					tableName     string
					offset, limit int = 0, 5
				)

				parsed := parsePath(r.URL.Path)
				tableName = parsed["table"]

				_ = checkIntQueryParam(r.URL.Query().Get("offset"), &offset)

				_ = checkIntQueryParam(r.URL.Query().Get("limit"), &limit)

				// table exists in tables field
				table := e.GetTable(tableName)

				query := fmt.Sprintf("SELECT * FROM %s LIMIT ?, ?;", tableName)
				rows, err := e.conn.Query(query, offset, limit)

				if err != nil {
					log.Println("select error:", err)
					JsonResponse(w, map[string]string{"error": err.Error()}, http.StatusInternalServerError)
					return
				}
				//*
				data := make([]interface{}, len(table.Columns))
				columns := make([]interface{}, len(table.Columns))
				for i := range columns {
					columns[i] = &data[i]
				}
				//*
				type record map[string]interface{}

				records := make([]record, 0)
				for rows.Next() {
					err := rows.Scan(columns...)
					if err != nil {
						if errors.Is(err, sql.ErrNoRows) {
							log.Println(err)
							http.Error(w, "record not found", http.StatusNotFound)
							return
						}
						log.Println(err)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}

					r := make(map[string]interface{}, len(columns))
					columnNames := func() []string {
						result := make([]string, 0, len(table.Columns))
						for _, column := range table.Columns {
							result = append(result, column.Name)
						}
						return result
					}()
					for i, columnName := range columnNames {
						if d, ok := data[i].([]byte); ok {
							data[i] = string(d)
						}
						r[columnName] = data[i]
					}
					records = append(records, r)
				}
				rows.Close()

				JsonResponse(w, map[string]map[string][]record{"response": {"records": records}}, http.StatusOK)
			}
			GetTableRecords(w, r)
		default:
			JsonResponse(w, map[string]string{}, http.StatusMethodNotAllowed)
		}
	case regexp.MustCompile(p2).MatchString(p):
		switch m {
		case http.MethodPut, http.MethodPost:
			e.Post(w, r)
		default:
			JsonResponse(w, map[string]string{}, http.StatusMethodNotAllowed)
		}
	case regexp.MustCompile(p3).MatchString(p):
		switch m {
		case http.MethodGet:
			GetRecordById := func(w http.ResponseWriter, r *http.Request) {
				var (
					id        int
					tableName string
					err       error
				)
				parsed := parsePath(r.URL.Path)
				tableName = parsed["table"]
				// if !exists {
				// 	http.Error(w, "unknown table", http.StatusNotFound)
				// 	return
				// }
				id, err = strconv.Atoi(parsed["id"])
				if err != nil {
					log.Println(err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				table := e.GetTable(tableName)
				// if !exists {
				// 	http.Error(w, "unknown table", http.StatusNotFound)
				// 	return
				// }
				// search field with substring 'id'
				// find primary key field and get its name
				var idParam string

				for _, column := range table.Columns {
					if !strings.Contains(column.Name, "id") {
						log.Println("no fields with 'id' substring")
						w.WriteHeader(http.StatusInternalServerError)
						return
					} else {
						idParam = column.Name
						break
					}
				}

				query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?;", tableName, idParam)
				row := e.conn.QueryRow(query, id)

				data := make([]interface{}, len(table.Columns))
				columns := make([]interface{}, len(table.Columns))
				for i := range columns {
					columns[i] = &data[i]
				}

				err = row.Scan(columns...)
				if err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						log.Println(err)
						http.Error(w, `{"error": "record not found"}`, http.StatusNotFound)
						return
					}
					log.Println(err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				records := make(map[string]interface{}, len(columns))
				for i, c := range table.Columns {
					if d, ok := data[i].([]byte); ok {
						data[i] = string(d)
					}
					records[c.Name] = data[i]
				}

				JsonResponse(w, map[string]map[string]map[string]interface{}{"response": {"record": records}}, http.StatusOK)
				return
			}
			GetRecordById(w, r)
		case http.MethodPut, http.MethodPost:
			// Post(w, r)
			e.Put(w, r)
		case http.MethodDelete:
			e.Delete(w, r)
		default:
			JsonResponse(w, map[string]string{}, http.StatusMethodNotAllowed)
		}
	default:
		JsonResponse(w, map[string]string{"error": "unknown table"}, http.StatusNotFound)
	}
}
