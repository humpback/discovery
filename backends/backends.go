package backends

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

/*
错误异常信息定义
*/
var (
	//ErrNotSupported 不支持的服务协议错误, 目前只支持(zk://|etcd://|consul://)
	ErrNotSupported = errors.New("backend not supported.")
	//ErrNotImplemented 未实现的发现服务
	ErrNotImplemented = errors.New("not implemented in this backend.")
	//ErrEntryInvlid 节点Entry无效，一般为编解码Entry失败
	ErrEntryInvlid = errors.New("entry invalid.")
	//ErrEntryKeyInvalid 节点Entry对应Key值无效
	ErrEntryKeyInvalid = errors.New("entry key invalid.")
	//ErrEntryKeyInvalid 节点Entry对应Key值无效
	ErrWatchException = errors.New("unexpected watch error.")
)

/*
Watcher 监视接口定义
提供在集群上监视节点的加入与离开功能, 由一个具体的Backend实体实现
*/
type Watcher interface {
	//stopCh: 传入一个非nil chan, 立即停止监视, 一般服务端需传入nil, 而命令方式查看一般传入非nil实体.
	//Entries：返回监视到的在线节点信息
	//error: 返回一个监视异常错误
	Watch(stopCh <-chan struct{}) (<-chan Entries, <-chan error)
}

/*
Backend 后端抽象接口定义
由一个Backend实体实现, 主要用于集群节点的发现管理和节点注册
*/
type Backend interface {
	//Watcher 每个后端必须实现Watcher接口, 监视节点变化
	Watcher
	//Initialize 初始化服务发现功能, 传入uris, heartbeat(心跳频率), ttl(超时阈值)和一个map选项设置
	Initialize(string, time.Duration, time.Duration, map[string]string) error
	//Register 注册到一个发现服务, 传入节点key, data(附加数据)
	Register(string, []byte) <-chan error
}

/*
backends 已实现 Backend interface 的对象
backend的装载在init()时由Register注册.
*/
var (
	backends = make(map[string]Backend)
)

/*
New 构建一个Backend实例
构造时根据scheme查找所带入的rawuri是否支持，不支持则返回ErrNotSupported错误
rawuri：后端服务发现路径
heartbeat: 心跳间隔
ttl: 节点过期阈值
configopts: 发现设置附加属性
*/
func New(rawuri string, heartbeat time.Duration, ttl time.Duration, configopts map[string]string) (Backend, error) {

	scheme, uris := parse(rawuri)
	if backend, exists := backends[scheme]; exists {
		err := backend.Initialize(uris, heartbeat, ttl, configopts)
		return backend, err
	}
	return nil, ErrNotSupported
}

/*
Register 注册发现后端类别
scheme："zk"、"etcd"、"consul"
Backend: 一个已构建的Backend实例
*/
func Register(scheme string, backend Backend) error {

	if _, exists := backends[scheme]; exists {
		return fmt.Errorf("backend scheme already registered %s", scheme)
	}
	backends[scheme] = backend
	return nil
}

/*
parse 截取rawuri段，返回backend类别和服务地址路径
*/
func parse(rawuri string) (string, string) {

	parts := strings.SplitN(rawuri, "://", 2)
	if len(parts) == 1 {
		return "zk", parts[0] //默认zk
	}
	return parts[0], parts[1]
}
