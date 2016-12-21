package mesos

import (
    "fmt"
    "time"
	"net/http"
    "errors"
    "log"
	"io"
	"io/ioutil"
	"encoding/json"
    "strings"
)


const (
    HTTP_GET = "GET"
)


type Client struct {
    config Config
	logger *log.Logger
	http *http.Client
}

type MesosClient interface {
    SetMasterURL(leader string)
    MasterURL() string
    BuildDiscoveryURL(uri string) string

    SlaveStateURL(hostname string) string
    SlaveStatsURL(hostname string) string

    DoApiRequest(url string, result interface{}) (int, string, error)
    unMarshallDataToJson(stream io.Reader, result interface{}) error
    DoRequst(method, url string)(int, string, *http.Response, error)
}


func NewClient(config Config) (*Client) {
    service := &Client{}
    service.config = config
    if config.LogOutput == nil {
        config.LogOutput = ioutil.Discard
    }
    service.logger = log.New(config.LogOutput, "[debug]: ", 0)
    service.http = &http.Client{
        Timeout: (time.Duration(config.RequestTimeout) * time.Second),
    }
    return service
}


func (c *Client) DoApiRequest(url string, result interface{}) (int, string, error)  {
    if status, content, _, err := c.doRequst(HTTP_GET, url); err != nil {
		return 0, "", err
    } else {
		if status >= 200 && status <= 299 {
			if result != nil {
				if err := c.unMarshallDataToJson(strings.NewReader(content), &result); err != nil {
					c.logger.Printf("doApiRequest(): failed to decode JSON, error: %s", err)
					return status, content, ErrInvalidResponse
				}
			}
			c.logger.Printf("apiCall() result: %V", result)
			return status, content, nil
		}

		switch status {
		case 500:
			return status, "", ErrInternalServerError
		case 404:
			return status, "", ErrDoesNotExist
		}
        return status, content, errors.New("Unknown error.")

    }
}


func (c *Client) unMarshallDataToJson(stream io.Reader, result interface{}) error {
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(result); err != nil {
		return err
	}
	return nil
}


func (c *Client) DoRequst(method, url string)(int, string, *http.Response, error) {
    client := &http.Client{}
    if request, err := http.NewRequest(method, url, nil); err != nil {
        c.logger.Printf("Unable to make call to Mesos: %s", err)
		return 0, "", nil, err
	} else {
		request.Header.Add("Content-Type", "application/json")
		var content string
		if response, err := client.Do(request); err != nil {
            log.Printf("Unable to make call to Mesos: %s", err)
            c.logger.Printf("Unable to make call to Mesos: %s", err)
            return 0, "", nil, errors.New("Unable to make call to Mesos")
		} else {
			c.logger.Printf("doRequst: %s, url: %s\n", method, url)
			if response.ContentLength != 0 {
				if response_content, err := ioutil.ReadAll(response.Body); err != nil {
					return response.StatusCode, "", response, err
				} else {
                    content = string(response_content)
                }
			}
			return response.StatusCode, content, response, nil
		}
	}
	return 0, "", nil, errors.New("Unable to make call to marathon")
}


func (c *Client) buildDiscoveryURL(uri string) string {
    return fmt.Sprintf("%s/%s", c.config.DiscoveryURL, uri)
}


func (c *Client) SetMasterURL(leader string) {
    c.config.MasterURL = fmt.Sprintf("%s//%s:%d", c.config.getScheme(), leader, c.config.MasterPort)
}


func (c *Client) MasterURL() string {
    return c.config.MasterURL
}


func (c *Client) SlaveStateURL(hostname string) string {
    return c.slaveURL(hostname, "slave(1)/state.json")
}


func (c *Client) SlaveStatsURL(hostname string) string {
    return c.slaveURL(hostname, "metrics/snapshot")
}

func (c *Client) SlaveURL(hostname, uri string) string {
    return fmt.Sprintf("%s//%s:%d/%s", c.config.getScheme(), hostname, c.config.SlavePort, uri)
}
