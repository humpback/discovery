package discovery

import "github.com/humpback/discovery/backends"
import _ "github.com/humpback/discovery/backends/kv"

import (
	"time"
)

type DiscoveryRegistryFunc func(key string, err error)
type DiscoveryWatchFunc func(added backends.Entries, removed backends.Entries, err error)

/*
发现对象结构定义
backend: 发现后端对象
*/
type Discovery struct {
	backend backends.Backend
}

/*
New 构造一个服务发现对象
rawuri：后端服务发现路径
heartbeat: 心跳间隔
ttl: 节点过期阈值
configopts: 发现设置附加属性
*/
func New(uris string, heartbeat time.Duration, ttl time.Duration, configopts map[string]string) (*Discovery, error) {

	backend, err := backends.New(uris, heartbeat, ttl, configopts)
	if err != nil {
		return nil, err
	}

	return &Discovery{
		backend: backend,
	}, nil
}

/*
Register 注册到集群服务发现, 由集群被管理节点调用
key: 集群节点唯一编码
buf: 节点附加数据, 可以为nil
stopCh: 退出心跳注册
Register为非阻塞方式, 上层业务调用后需考虑阻塞, 避免应用退出.
*/
func (d *Discovery) Register(key string, buf []byte, stopCh <-chan struct{}, fn DiscoveryRegistryFunc) {

	errCh := d.backend.Register(key, buf, stopCh)
	go func() {
		for {
			select {
			case err := <-errCh:
				if fn != nil {
					fn(key, err)
				}
				if err == backends.ErrEntryKeyInvalid || err == backends.ErrEntryInvlid || err == backends.ErrRegistLoopQuit {
					return
				}
			}
		}
	}()
}

/*
Watch 集群监视功能, 由集群管理节点调用
Watch 为非阻塞方式, 上层业务调用后需考虑阻塞, 避免应用退出.
stopCh: 退出服务发现
*/
func (d *Discovery) Watch(stopCh <-chan struct{}, fn DiscoveryWatchFunc) {

	discoveryCh, errCh := d.backend.Watch(stopCh)
	go func() {
		cache := backends.Entries{}
		for {
			select {
			case entries := <-discoveryCh:
				{
					added, removed := cache.Diff(entries)
					cache = entries
					if fn != nil {
						fn(added, removed, nil)
					}
				}
			case err := <-errCh:
				{
					if fn != nil {
						fn(nil, nil, err)
					}
				}
			}
		}
	}()
}
