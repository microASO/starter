package getter

import (
    "fmt"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/rpc"
	"net/url"
)

// RequestHandler ...
func RequestHandler(reqURL string, uri string, verb string, cert string, key string) (string, error) {

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

	req, _ := http.NewRequest(verb, reqURL+uri, nil)

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

type description struct {
	Columns []string `json:"columns"`
}

// RestOutput translate REST response
type RestOutput struct {
	Result [][]interface{} `json:"result"`
	Desc   description     `json:"desc"`
}

// ResultSchema ...
//u'desc': {u'columns': [u'tm_id', u'tm_username', u'tm_taskname',
//u'tm_destination', u'tm_destination_lfn', u'tm_source',
//u'tm_source_lfn', u'tm_filesize', u'tm_publish', u'tm_jobid',
//u'tm_job_retry_count', u'tm_type', u'tm_aso_worker', u'tm_transfer_retry_count', u'tm_transfer_max_retry_count', u'tm_publication_retry_count', u'tm_publication_max_retry_count', u'tm_rest_host', u'tm_rest_uri', u'tm_transfer_state', u'tm_publication_state', u'tm_transfer_failure_reason', u'tm_publication_failure_reason', u'tm_fts_id', u'tm_fts_instance', u'tm_last_update', u'tm_start_time', u'tm_end_time', u'tm_user_role', u'tm_user_group',
// u'tm_input_dataset', u'tm_cache_url', u'tm_dbs_url']}}
type ResultSchema struct {
	FileID         string `json:"tm_id"`
	User           string `json:"tm_username"`
	Role           string `json:"tm_role"`
	Group          string `json:"tm_group"`
	Taskname       string `json:"tm_taskname"`
	Destination    string `json:"tm_destination"`
	DestinationLfn string `json:"tm_destination_lfn"`
	SourceLfn      string `json:"tm_source_lfn"`
	JobType        string `json:"tm_type"`
	WorkerName     string `json:"tm_aso_worker"`
	InputDataset   string `json:"tm_input_dataset"`
	CacheURL       string `json:"tm_cache_url"`
	DBSUrl         string `json:"tm_dbs_url"`
	FileSize       int    `json:"tm_filesize"`
	ToPublish      int    `json:"tm_publish"`
}

// SplitFiles orders files by user
func SplitFiles(input RestOutput, bulk chan []ResultSchema, logger *log.Logger) {
	// translate REST response
	output := make([]map[string]interface{}, len(input.Result))
	tmpOut := make(map[string]interface{})
	files := make([]ResultSchema, len(input.Result))
	var user []string
	var users [][]string
	tasks := make(map[string][]ResultSchema)

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

		// create unique list of users
		user = []string{files[i].User, files[i].Group, files[i].Role, files[i].Taskname}
		duplicated := false
		if len(users) != 0 {
			for u := range users {
				tmpDuplex := true
				for i := range user {
					if user[i] != users[u][i] {
						tmpDuplex = false
					}
				}
				if tmpDuplex {
					duplicated = true
				}
			}
		}

		// split payload per unique tasks
		if !duplicated && files[i].ToPublish == 1 {
			users = append(users, user)
			tasks[files[i].Taskname] = make([]ResultSchema, 0)
		}
		tasks[files[i].Taskname] = append(tasks[files[i].Taskname], files[i])
	}

	// log active users and delivery payloads
	logger.Print(users)
	for k := range tasks {
		logger.Printf("delivering payloads for task %s", k)
		bulk <- tasks[k]
	}

}

// SendTask ..
func SendTask(ch chan []ResultSchema, reqURL string, logger *log.Logger) error {

	payload := <-ch
	logger.Printf("Sending payload %s to the publisher", payload[0].Taskname)

	conn, err := rpc.Dial("tcp", reqURL)
	if err != nil {
		logger.Fatalln("error: ", err)
	}

	// TODO: need timeout server side
	var result int64

	toRPC := RPCArgs{payload}
	err = conn.Call("Server.Publish", toRPC, &result)
	if err != nil {
		logger.Fatal(err)
	} else {
		logger.Printf("Server.Publish result: %d ", result)
	}
	conn.Close()

	return nil
}

// Server ...
type Server struct{}

// RPCArgs ...
type RPCArgs struct {
	Payload []ResultSchema
}

// UserDNOutput ...
type UserDNOutput struct {
	Result [][]string `json:"result"`
}

// Publish ...
func (myself *Server) Publish(args *RPCArgs, reply *int64) error {
	payload := args.Payload
    // TODO: include logger fixing 'log.Logger has no exported fields'
	//logger := args.Logger

	// get user proxy from proxy cache
	//  - get dn
	//  - retrieve proxy from proxy cache

	// get user DN from
	reqURL := "https://cmsweb.cern.ch/sitedb/data/prod/people"
	//v := url.Values{}
	data := url.Values{"match": {payload[0].User}}.Encode()
	//data := "match" + payload[0].User

	fmt.Print("Retrieving user DN \n")
	response, err := RequestHandler(reqURL, "?"+data, "GET", "proxy", "proxy")
	if err != nil {
		fmt.Printf("Error retrieving user DN with %s", reqURL+"?"+data)
		return err
	}
	fmt.Print("User DN retrieved \n")

	var responseDN UserDNOutput
	json.Unmarshal([]byte(response), &responseDN)

	//curl -ks --cert $X509_USER_PROXY --key $X509_USER_PROXY "https://cmsweb.cern.ch/sitedb/data/prod/people?match=jbalcas"
	//{"desc": {"columns": ["username", "email", "forename", "surname", "dn", "phone1", "phone2", "im_handle"]}, "result": [
	//["jbalcas", "justas.balcas@cern.ch", "Justas", "Balcas", "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=jbalcas/CN=751133/CN=Justas Balcas", null, null, null]
	//]}
	sitedbDN := responseDN.Result[0][4]
	fmt.Printf("Usuer DN: %s \n", sitedbDN)
	// REST GET proxy from proxy cache

	// get task status

	// if status terminal or len>tot go ahead

	// 	get metadata (getPublDescFiles)
    // TODO fix automatic getting url
	// urlCache := payload[0].CacheURL
    urlCache := "https://cmsweb-testbed.cern.ch/crabserver/preprod/filemetadata"
	// TODO: url encode parameters later
	queryURL := "taskname=" + url.QueryEscape(payload[0].Taskname) + "&filetype=EDM"

    response, err = RequestHandler(urlCache, "?"+queryURL, "GET", "proxy", "proxy")
    if err != nil {
        fmt.Printf("Error retrieving file metadata with %s", urlCache+"?"+queryURL)
        return err
    }
    fmt.Printf("Response: %s", response)

	// sendo to publisher

	*reply = 0
	fmt.Printf("server query: %s \n", queryURL)
	fmt.Printf("server cache url: %s \n", urlCache)

	return nil
}
