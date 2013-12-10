package seriesly

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

type RestClient struct {
	client *http.Client
}

func (c *RestClient) Do(req *http.Request) (mresp map[string]interface{}) {
	defer func() {
		if err := recover(); err != nil {
			log.Fatal(err)
		}
	}()

	resp, err := c.client.Do(req)
	if err != nil {
		log.Printf("%v", err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("%v", err)
	}
	json.Unmarshal(body, &mresp)
	return
}

type SerieslyClient struct {
	baseURI string
	client  *RestClient
}

const MaxIdleConnsPerHost = 100

func (c *SerieslyClient) Init(hostname, db string) {
	c.baseURI = fmt.Sprintf("http://%s:3133/%s", hostname, db)
	t := &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
	c.client = &RestClient{&http.Client{Transport: t}}

	c.CreateDb()
}

func (c *SerieslyClient) CreateDb() {
	req, _ := http.NewRequest("PUT", c.baseURI, nil)
	c.client.Do(req)
}

func (c *SerieslyClient) Append(ts int64, samples map[string]interface{}) {
	b, _ := json.Marshal(samples)
	j := bytes.NewReader(b)
	uri := fmt.Sprintf("%s?ts=%d", c.baseURI, ts)
	req, _ := http.NewRequest("POST", uri, j)

	c.client.Do(req)
}
