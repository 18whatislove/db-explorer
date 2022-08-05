package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
)

func GetTableNames(e *DbExplorer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tableNames := make([]string, 0, len(e.Tables))
		for _, table := range e.Tables {
			tableNames = append(tableNames, table.Name)
		}
		JsonResponse(w, map[string]map[string][]string{"response": {"tables": tableNames}}, http.StatusOK)
	}
}

func parsePath(path string) map[string]string {
	args := strings.Split(strings.Trim(path, "/"), "/")
	parsedData := make(map[string]string, 2)
	if len(args) != 0 {
		parsedData["table"] = args[0]
		if len(args) == 2 {
			parsedData["id"] = args[1]
		}
	}
	return parsedData
}

func ParamsValidation(params map[string]interface{}, cols []*Column, flags map[string]bool) error {
	var (
		err    error
		ignore bool = flags["ignore"]
	)
LOOP:
	for _, col := range cols {
		// discard primary key auto increment field
		if value, exists := params[col.Name]; exists {
			if ignore && col.IsPK {
				delete(params, col.Name)
				continue
			}
			if col.IsPK {
				err = fmt.Errorf("field %s have invalid type", col.Name)
				break
			}
			if value == nil {
				if !col.NullAble {
					err = fmt.Errorf("field %s have invalid type", col.Name)
					break
				}
				continue
			}

			switch t := col.Type; t {
			case "string":
				_, ok := value.(string)
				if !ok {
					err = fmt.Errorf("field %s have invalid type", col.Name)
					break LOOP
				}
			case "int":
				_, ok := value.(float64)
				if !ok {
					err = fmt.Errorf("field %s have invalid type", col.Name)
					break LOOP
				}
			case "bool":
				_, ok := value.(bool)
				if !ok {
					err = fmt.Errorf("field %s have invalid type", col.Name)
					break LOOP
				}
			}
		}
	}
	return err
}

func JsonResponse(w http.ResponseWriter, data interface{}, status int) {
	response, err := json.Marshal(&data)
	if err != nil {
		log.Println("JsonResponse:", err)
		w.WriteHeader(http.StatusBadRequest)
		panic(err)
	}
	w.WriteHeader(status)
	w.Write(response)
}

// Check limit and offset param values
func checkIntQueryParam(in string, out *int) error {
	if in == "" {
		return nil
	}
	value, err := strconv.Atoi(in)
	if err != nil {
		log.Println(err, "args:", in)
		return err
	}
	*out = value
	return nil
}
