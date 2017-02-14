package kv

import "github.com/docker/libkv"
import "github.com/docker/libkv/store"
import "github.com/docker/libkv/store/consul"
import "github.com/docker/libkv/store/etcd"
import "github.com/docker/libkv/store/zookeeper"
import "github.com/humpback/discovery/backends"

import (
	"fmt"
	"path"
	"strings"
	"sync"
	"time"
)

/*
构建时若不指定configopts情况
发现默认路径为defaultDiscoveryPath值
*/
const (
	defaultDiscoveryPath = "discovery/service"
)

/*
Discovery 发现服务结构定义
集成LibStore库实现
*/
type Discovery struct {
	backend   store.Backend
	store     store.Store
	heartbeat time.Duration
	ttl       time.Duration
	prefix    string
	path      string
}

/*
init 初始化libkv包并构建Discovery实例
只实现了zookeeper,consul与etcd三种方式
*/
func init() {

	//注册默认支持libkv库
	zookeeper.Register()
	consul.Register()
	etcd.Register()

	//注册backend
	backends.Register("zk", &Discovery{backend: store.ZK})
	backends.Register("consul", &Discovery{backend: store.CONSUL})
	backends.Register("etcd", &Discovery{backend: store.ETCD})
}

/*
Initialize 初始化一个Discovery实例
根据backend类型构造出libkv的Store
*/
func (d *Discovery) Initialize(uris string, heartbeat time.Duration, ttl time.Duration, configopts map[string]string) error {

	var (
		parts = strings.SplitN(uris, "/", 2)
		addrs = strings.Split(parts[0], ",")
		err   error
	)

	if len(parts) == 2 {
		d.prefix = parts[1]
	}

	d.heartbeat = heartbeat
	d.ttl = ttl
	dpath := defaultDiscoveryPath
	if configopts["kv.path"] != "" {
		dpath = strings.TrimSpace(configopts["kv.path"])
	}

	d.path = path.Join(d.prefix, dpath, "nodes")
	d.store, err = libkv.NewStore(d.backend, addrs, &store.Config{})
	return err
}

/*
Register 服务注册
节点根据heartbeat轮询定时注册, 节点过期由ttl阈值决定
key: 集群节点唯一编码
data: 节点数据，可为nil
stopCh: 退出心跳注册
*/

func (d *Discovery) Register(key string, data []byte, stopCh <-chan struct{}) <-chan error {

	errCh := make(chan error)
	go func() {
		defer close(errCh)
		if strings.TrimSpace(key) == "" {
			errCh <- backends.ErrEntryKeyInvalid
			return
		}

		opts := &store.WriteOptions{TTL: d.ttl}
		entry := &backends.Entry{Key: key, Data: data}
		buf, err := backends.EnCodeEntry(entry)
		if err != nil {
			errCh <- backends.ErrEntryInvlid
			return
		}

		for {
			t := time.NewTicker(d.heartbeat)
			select {
			case <-t.C:
				{
					t.Stop()
					if err := d.store.Put(path.Join(d.path, key), buf, opts); err != nil {
						errCh <- err
					}
				}
			case <-stopCh:
				{
					t.Stop()
					errCh <- backends.ErrRegistLoopQuit
					return
				}
			}
		}
	}()
	return errCh
}

/*
Watch 节点监视
由发现服务端调用, 实现WatchTree监视所有节点变化
stopCh: 退出服务发现
*/
func (d *Discovery) Watch(stopCh <-chan struct{}) (<-chan backends.Entries, <-chan error) {

	ch := make(chan backends.Entries)
	errCh := make(chan error)
	go func() {
		defer close(ch)
		defer close(errCh)
		for {
			exists, err := d.store.Exists(d.path)
			if err != nil {
				errCh <- err
			}
			if !exists {
				if err := d.store.Put(d.path, []byte(""), &store.WriteOptions{IsDir: true}); err != nil {
					errCh <- err
				}
			}
			watchCh, err := d.store.WatchTree(d.path, stopCh)
			if err != nil {
				errCh <- err
			} else {
				if !d.watchOnce(stopCh, watchCh, ch, errCh) {
					return
				}
			}
			errCh <- backends.ErrWatchException
			time.Sleep(d.heartbeat)
		}
	}()
	return ch, errCh
}

func (d *Discovery) watchOnce(stopCh <-chan struct{}, watchCh <-chan []*store.KVPair, discoveryCh chan backends.Entries, errCh chan error) bool {

	for {
		select {
		case pairs := <-watchCh:
			{
				if pairs == nil {
					return true
				}

				pCall := struct {
					sync.Mutex
					Data [][]byte
				}{
					Data: make([][]byte, 0),
				}

				size := len(pairs)
				wgroup := sync.WaitGroup{}
				wgroup.Add(size)
				for _, it := range pairs {
					go func(p *store.KVPair) {
						path := d.path + "/" + p.Key
						pair, err := d.store.Get(path)
						if err != nil {
							fmt.Printf("watch error:%s | %s\n", path, err.Error())
						} else {
							pCall.Lock()
							pCall.Data = append(pCall.Data, pair.Value)
							pCall.Unlock()
						}
						wgroup.Done()
					}(it)
				}
				wgroup.Wait()
				entries, err := backends.PressEntriesData(pCall.Data)
				if err != nil {
					errCh <- err
				} else {
					discoveryCh <- entries
				}
			}
		case <-stopCh:
			{
				return false
			}
		}
	}
}
