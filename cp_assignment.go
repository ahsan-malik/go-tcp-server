package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

type data struct {
	Positive string `json:"positive"`
	Test     string `json:"test"`
	Date     string `json:"date"`
	Discharg string `json:"discharge"`
	Expire   string `json:"expire"`
	Admit    string `json:"admit"`
	Region   string `json:"region"`
}

//Response parent element of data array
type response struct {
	Response []data `json:"response"`
}

//DataRequest used to query dataset
type DataRequest struct {
	Query query `json:"query"`
}

//Query for nested json
type query struct {
	Date   string `json:"date"`
	Region string `json:"region"`
}

func main() {

	records := readCsvFile("data.csv")

	// create a listener for provided network and host address
	ln, err := net.Listen("tcp", ":4040")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer ln.Close()

	fmt.Println("Server is listening....")

	// connection loop
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			conn.Close()
			continue
		}
		log.Println("Connected to ", conn.RemoteAddr())
		if _, err := conn.Write([]byte("Connected with the server...\nIn order to search write query in json format like this:\nUsage: {query:{region:sindh}}    !enclose each word in double qoutes!")); err != nil {
			log.Println("Error writing:", err)
			conn.Close()
			continue
		}
		go handleConnection(conn, records)
	}
}

// handle client connection
func handleConnection(conn net.Conn, records []data) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Println("error closing connection:", err)
		}
	}()

	// create json encoder/decoder using the io.Conn as
	// io.Writer and io.Reader for streaming IO
	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)

	// command-loop
	for {
		// Next decode the incoming data into Go value
		var req DataRequest
		if err := dec.Decode(&req); err != nil {
			log.Println("failed to unmarshal request:", err)
			return
		}

		// search records, result is []data
		var rsp response
		rsp.Response = findData(records, req.Query.Region, req.Query.Date)

		// encode result to JSON array
		enc.SetIndent("", "\t")
		if err := enc.Encode(&rsp); err != nil {
			log.Println("failed to encode data:", err)
			return
		}
	}
}

func findData(table []data, region, date string) []data {
	if region == "" && date == "" {
		return nil
	}

	result := make([]data, 0)

	if region == "" {
		for _, _data := range table {
			if strings.Contains(_data.Date, date) {
				result = append(result, _data)
			}
		}
	} else if date == "" {
		region = strings.ToUpper(region)
		for _, _data := range table {
			if strings.Contains(strings.ToUpper(_data.Region), region) {
				result = append(result, _data)
			}
		}
	}
	return result
}

//reading csv
func readCsvFile(filePath string) []data {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)

	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}

	var _data data
	var dataArray []data
	for _, each := range records {

		_data.Positive = each[0]
		_data.Test = each[1]
		_data.Date = each[2]
		_data.Discharg = each[3]
		_data.Expire = each[4]
		_data.Admit = each[6]
		_data.Region = each[5]

		dataArray = append(dataArray, _data)
	}
	return dataArray
}
