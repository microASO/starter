package getter

import (
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

type description struct {
	Columns []string `json:"columns"`
}

// RestOutput translate REST response
type RestOutput struct {
	Result [][]interface{} `json:"result"`
	Desc   description     `json:"desc"`
}

// ResultSchema ...
type ResultSchema struct {
	FileID   string `json:"tm_id"`
	User     string `json:"tm_username"`
	Role     string `json:"tm_role"`
	Group    string `json:"tm_group"`
	Taskname string `json:"tm_taskname"`
}

//u'desc': {u'columns': [u'tm_id', u'tm_username', u'tm_taskname',
//u'tm_destination', u'tm_destination_lfn', u'tm_source',
//u'tm_source_lfn', u'tm_filesize', u'tm_publish', u'tm_jobid',
//u'tm_job_retry_count', u'tm_type', u'tm_aso_worker', u'tm_transfer_retry_count', u'tm_transfer_max_retry_count', u'tm_publication_retry_count', u'tm_publication_max_retry_count', u'tm_rest_host', u'tm_rest_uri', u'tm_transfer_state', u'tm_publication_state', u'tm_transfer_failure_reason', u'tm_publication_failure_reason', u'tm_fts_id', u'tm_fts_instance', u'tm_last_update', u'tm_start_time', u'tm_end_time', u'tm_user_role', u'tm_user_group',
// u'tm_input_dataset', u'tm_cache_url', u'tm_dbs_url']}}

// SplitFiles orders files by user
func SplitFiles(input RestOutput, bulk chan []map[string]interface{}, logger *log.Logger) {
	// translate REST response
	output := make([]map[string]interface{}, len(input.Result))
	tmpOut := make(map[string]interface{})
	files := make([]ResultSchema, len(input.Result))
	var user []string
	var users [][]string

	for i := range input.Result {
		//logger.Print(len(output))
		//logger.Print(i)
		for key := range input.Desc.Columns {
			//logger.Print(input.Desc.Columns[key])
			//logger.Print(input.Result[i][key])
			tmpOut[input.Desc.Columns[key]] = input.Result[i][key]
			output[i] = tmpOut
		}
		dumpBytes, _ := json.Marshal(output[i])
		_ = json.Unmarshal(dumpBytes, &files[i])
		//logger.Print(docs[i].FileID)
		user = []string{files[i].User, files[i].Group, files[i].Role}
		if len(users) != 0 {
			for u := range users {
				if user[0] != users[u][0] {
					users = append(users, user)
				}
			}
		} else {
			users = append(users, user)
		}
		logger.Print(users)

	}

	bulk <- output

}

// SendTask ..
func SendTask(ch chan []map[string]interface{}, url string, logger *log.Logger) error {

	//printit, _ := json.Marshal(<-ch)
	//logger.Print(string(printit))
	/*
		conn, err := net.Dial("tcp", url)
		if err != nil {
			logger.Fatal(err)
		}
		data := <-ch
		err = json.NewEncoder(conn).Encode(data)
		if err != nil {
			logger.Fatal(err)
		}
		conn.Close()*/

	return nil
}

// RequestHandler ...
func RequestHandler(url string, uri string, verb string, cert string, key string) (string, error) {

	// Load client cert
	certificate, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return "", err
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{certificate},
		InsecureSkipVerify: true,
	}

	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}

	req, _ := http.NewRequest(verb, url+uri, nil)

	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Dump response
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
