package main

import (
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/robfig/cron/v3"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	indexSettingsDefault = `{
		"settings": {
			"number_of_shards": 5,
			"number_of_replicas": 1,
			"refresh_interval": "30s",
			"translog": {
				"flush_threshold_size": "512m",
				"durability": "async"
			}
		},
		"mappings": {
			"properties": {
				"timestamp": {
					"type": "date",
					"format": "epoch_millis"
				}
			}
		}
	}`

	indexSettingsSpecial = `{
		"settings": {
			"number_of_shards": 5,
			"number_of_replicas": 1,
			"refresh_interval": "30s",
			"translog": {
				"flush_threshold_size": "512m",
				"durability": "async"
			}
		},
		"mappings": {
			"properties": {
				"sendTimeStamp": {
					"type": "date",
					"format": "epoch_millis"
				}
			}
		}
	}`

	indexSettingsQuery = `{
		"settings": {
			"number_of_shards": 5,
			"number_of_replicas": 1,
			"refresh_interval": "30s",
			"translog": {
				"flush_threshold_size": "512m",
				"durability": "async"
			}
		},
		"mappings": {
			"properties": {
				"queryTimeStamp": {
					"type": "date",
					"format": "epoch_millis"
				}
			}
		}
	}`
	retryCount     = 3
	retryInterval  = 2 * time.Second
	requestTimeout = 10 * time.Second
)

func AddIndex() {
	currentDate := time.Now()

	elasticsearchHost := os.Getenv("ELASTICSEARCH_HOST")
	elasticsearchPortStr := os.Getenv("ELASTICSEARCH_PORT")
	elasticsearchUsername := os.Getenv("ELASTICSEARCH_USERNAME")
	elasticsearchPassword := os.Getenv("ELASTICSEARCH_PASSWORD")
	elasticsearchPort, err := strconv.Atoi(elasticsearchPortStr)
	if err != nil {
		fmt.Printf("转换端口时出错：%s\n", err)
		return
	}
	indexesString := os.Getenv("INDEXES")
	speciallist := os.Getenv("SpecialList")
	querylist := os.Getenv("QueryList")

	if err != nil {
		fmt.Printf("转换端口时出错：%s\n", err)
		return
	}
	cfg := elasticsearch.Config{
		Addresses: []string{
			fmt.Sprintf("http://%s:%d", elasticsearchHost, elasticsearchPort),
		},
		Username: elasticsearchUsername,
		Password: elasticsearchPassword,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 10,
		},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		fmt.Printf("创建客户端时出错：%s\n", err)
		return
	}

	specialIndexList := strings.Split(speciallist, ",")
	queryIndexList := strings.Split(querylist, ",")
	indexes := strings.Split(indexesString, ",")
	for _, index := range indexes {
		indexDate := currentDate.AddDate(0, 0, 1)
		indexName := fmt.Sprintf("%s%s", index, indexDate.Format("20060102"))
		createIndexes(client, []string{indexName}, specialIndexList, queryIndexList)
	}
}

func createIndexes(client *elasticsearch.Client, indexNames []string, specialIndexList, queryIndexList []string) {
	for _, indexName := range indexNames {
		var exists bool
		err := retry(retryCount, retryInterval, func() error {
			var err error
			exists, err = indexExists(client, indexName)
			return err
		})
		if err != nil {
			fmt.Printf("检查索引是否存在时出错：%s\n", err)
			continue
		}
		if exists {
			fmt.Printf("%s 索引已存在。\n", indexName)
		} else {
			var indexSettings string
			if contains(specialIndexList, indexName) {
				indexSettings = indexSettingsSpecial
			} else if contains(queryIndexList, indexName) {
				indexSettings = indexSettingsQuery
			} else {
				indexSettings = indexSettingsDefault
			}

			err := retry(retryCount, retryInterval, func() error {
				return createIndexWithMapping(client, indexName, indexSettings)
			})
			if err != nil {
				fmt.Printf("创建索引时出错：%s\n", err)
			}
		}
	}
}

func indexExists(client *elasticsearch.Client, indexName string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	res, err := client.Indices.Exists([]string{indexName}, client.Indices.Exists.WithContext(ctx))
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return false, nil
	} else if res.IsError() {
		return false, fmt.Errorf("检查索引时出错：%s", res.Status())
	}
	return true, nil
}

func createIndexWithMapping(client *elasticsearch.Client, indexName, mapping string) error {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	req := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(mapping),
	}

	res, err := req.Do(ctx, client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		if res.StatusCode == http.StatusBadRequest && strings.Contains(string(body), "resource_already_exists_exception") {
			fmt.Printf("索引创建成功：%s\n", indexName)
			return nil
		}
		return fmt.Errorf("索引创建失败：%s，响应体：%s", res.Status(), body)
	}
	fmt.Printf("索引已创建：%s\n", indexName)
	return nil
}

func retry(attempts int, sleep time.Duration, fn func() error) error {
	for i := 0; i < attempts; i++ {
		if err := fn(); err != nil {
			if i < (attempts - 1) {
				time.Sleep(sleep)
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("超过最大重试次数")
}

func contains(list []string, item string) bool {
	for _, v := range list {
		if strings.Contains(item, v) {
			return true
		}
	}
	return false
}

func main() {
	c := cron.New()
	crontime := os.Getenv("CRONTIME")
	if crontime == "" {
		crontime = "* * * * *"
	}

	_, err := c.AddFunc(crontime, func() {
		fmt.Println("定时任务执行于:", time.Now())
		AddIndex()
	})
	if err != nil {
		fmt.Printf("添加定时任务失败：%s\n", err)
		return
	}

	c.Start()

	select {}
}
