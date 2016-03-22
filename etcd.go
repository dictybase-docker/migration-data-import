package main

import (
	"fmt"
	"regexp"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"gopkg.in/codegangsta/cli.v1"
)

func waitForEtcd(key string, c *cli.Context) error {
	api, err := getEtcdAPIHandler(c)
	if err != nil {
		return err
	}
	_, err = api.Get(context.Background(), key, nil)
	if err != nil {
		if m, _ := regexp.MatchString("100", err.Error()); m {
			// key is not present have to watch it
			log.WithFields(log.Fields{
				"type": "etcd",
				"key":  key,
				"kind": "etcd-wait",
			}).Info("start watch for etcd key")
			w := api.Watcher(key, &client.WatcherOptions{AfterIndex: 0, Recursive: false})
			_, err := w.Next(context.Background())
			if err != nil {
				return err
			}
			return nil
		} else {
			return err
		}
	}
	// key is already present
	return nil
}

func getEtcdAPIHandler(c *cli.Context) (client.KeysAPI, error) {
	cfg := client.Config{
		Endpoints: []string{getEtcdURL(c)},
		Transport: client.DefaultTransport,
	}
	cl, err := client.New(cfg)
	if err != nil {
		return nil, err
	}
	return client.NewKeysAPI(cl), nil
}

func registerWithEtcd(key string, c *cli.Context) error {
	api, err := getEtcdAPIHandler(c)
	if err != nil {
		return err
	}
	_, err = api.Create(context.Background(), key, "complete")
	if err != nil {
		return err
	}
	return nil
}

func getEtcdURL(c *cli.Context) string {
	return fmt.Sprintf("http://%s:%s", c.GlobalString("etcd-host"), c.GlobalString("etcd-port"))
}

func hasEtcd(c *cli.Context) bool {
	if len(c.GlobalString("etcd-host")) > 1 && len(c.GlobalString("etcd-port")) > 1 {
		return true
	}
	return false
}
