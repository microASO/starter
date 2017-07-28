package getter

import (
	"crypto/tls"
	"io/ioutil"
	"log"
	"net/http"
)

// GetFiles ...
func GetFiles(ch chan []byte, i []byte) {
	ch <- i
}

// ProvidePublication ...
func ProvidePublication(test []byte) ([]byte, error) {
	return test, nil
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
	log.Println(string(data))
	return string(data), nil
}
