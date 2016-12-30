package backends

import (
	"bytes"
	"encoding/json"
)

/*
Entry 节点信息
Key:  集群节点唯一编码
Data: 节点附加数据, 可为nil
*/
type Entry struct {
	Key  string `json:"key"`
	Data []byte `json:"data"`
}

/*
NewEntry 创建一个新的节点信息
*/
func NewEntry(key string, data []byte) *Entry {

	return &Entry{
		Key:  key,
		Data: data,
	}
}

/*
EnCodeEntry 编码一个Entry数据
*/
func EnCodeEntry(entry *Entry) ([]byte, error) {

	buf := bytes.NewBuffer([]byte{})
	if err := json.NewEncoder(buf).Encode(entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

/*
DeCodeEntry 解码一个Entry数据
*/
func DeCodeEntry(data []byte) (*Entry, error) {

	entry := &Entry{}
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(entry); err != nil {
		return nil, err
	}
	return entry, nil
}

/*
Equals 判断两个Entry是否相等
*/
func (entry *Entry) Equals(cmp *Entry) bool {

	if cmp != nil {
		return entry.Key == cmp.Key
	}
	return false
}

/*
String 返回Entry字符串形式
该函数只返回Key值
*/
func (entry *Entry) String() string {

	return entry.Key
}

/*
Entries 节点切片定义
代表多个Entry节点信息
*/
type Entries []*Entry

/*
Contains 查找entry在entries中是否存在
*/
func (entries Entries) Contains(entry *Entry) bool {

	if entry != nil {
		for _, it := range entries {
			if it.Equals(entry) {
				return true
			}
		}
	}
	return false
}

/*
Diff 比较两个Entries差异
added：新加入节点
removed: 删除的节点
*/
func (entries Entries) Diff(cmp Entries) (Entries, Entries) {

	added := Entries{}
	for _, entry := range cmp { //查找出新加入的节点
		if !entries.Contains(entry) {
			added = append(added, entry)
		}
	}

	removed := Entries{}
	for _, entry := range entries { //查找出被删除的节点
		if !cmp.Contains(entry) {
			removed = append(removed, entry)
		}
	}
	return added, removed
}

/*
PressEntriesData 将数据集转换并返回一个[]*Entry
data: 多个节点数据信息
一但有节点数据转换失败，则返回nil
*/
func PressEntriesData(data [][]byte) (Entries, error) {

	entries := Entries{}
	if data == nil {
		return entries, nil
	}
	for _, it := range data {
		if it == nil || len(it) == 0 {
			continue
		}
		entry, err := DeCodeEntry(it)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
