package merkledag

import (
	"encoding/json"
	"fmt"
	"hash"
	"math"
)

const (
	KB          = 1 << 10
	ChunkSize   = 256 * KB
	MaxListLine = 4096
	BLOB        = "blob"
	LINK        = "link"
	TREE        = "tree"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []*Link // 使用指针切片存储Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) []byte {
	// TODO 将分片写入到KVStore中，并返回Merkle Root
	var obj Object
	switch node.Type() {
	case FILE:
		obj = *handleFile(node, store, h) // 解引用指针获取实际对象
		break
	case DIR:
		obj = *handleDir(node, store, h) // 解引用指针获取实际对象
		break
	}
	JsonObj, _ := json.Marshal(obj)
	return computeHash(obj, JsonObj, h)
}

func handleFile(node Node, store KVStore, h hash.Hash) *Object {
	obj := &Object{} // 使用指针类型的Object
	FileNode, _ := node.(File)
	if FileNode.Size() > ChunkSize {
		numChunks := math.Ceil(float64(FileNode.Size()) / float64(ChunkSize))
		height := 0
		tmp := numChunks
		for {
			height++
			tmp /= MaxListLine
			if tmp == 0 {
				break
			}
		}
		obj, _ = dfsHandleFile(height, FileNode, store, 0, h) // 直接返回指针
	} else {
		obj.Data = FileNode.Bytes()
		putObjInStore(obj, store, h)
	}
	return obj
}

func handleDir(node Node, store KVStore, h hash.Hash) *Object {
	dirNode, _ := node.(Dir)
	iter := dirNode.It()
	treeObject := &Object{} // 使用指针类型的Object
	for iter.Next() {
		node := iter.Node()
		switch node.Type() {
		case FILE:
			file := node.(File)
			tmp := handleFile(node, store, h)
			jsonMarshal, _ := json.Marshal(tmp)
			treeObject.Links = append(treeObject.Links, &Link{ // 存储Link指针
				Hash: computeHash(*tmp, jsonMarshal, h),
				Size: int(file.Size()),
				Name: file.Name(),
			})
			if tmp.Links == nil {
				treeObject.Data = append(treeObject.Data, []byte(BLOB)...)
			} else {
				treeObject.Data = append(treeObject.Data, []byte(LINK)...)
			}

			break
		case DIR:
			dir := node.(Dir)
			tmp := handleDir(node, store, h)
			jsonMarshal, _ := json.Marshal(tmp)
			treeObject.Links = append(treeObject.Links, &Link{ // 存储Link指针
				Hash: computeHash(*tmp, jsonMarshal, h),
				Size: int(dir.Size()),
				Name: dir.Name(),
			})
			treeObject.Data = append(treeObject.Data, []byte(TREE)...)
			break
		}
	}
	putObjInStore(treeObject, store, h)
	return treeObject
}

func computeHash(obj Object, data []byte, h hash.Hash) []byte {
	if len(obj.Links) == 0 {
		h.Reset()
		h.Write(data)
	} else {
		hash := make([]byte, 0)
		for i := 0; i < len(obj.Links); i++ {
			h.Reset()
			hash = append(hash, obj.Links[i].Hash...)
			h.Write(hash)
		}
	}

	return h.Sum(nil)
}

func dfsHandleFile(height int, node File, store KVStore, start int, h hash.Hash) (*Object, int) {
	obj := &Object{} // 使用指针类型的Object
	lenData := 0
	// 处理多层分片
	for i := 1; i <= MaxListLine && start < len(node.Bytes()); i++ {
		var tmpObj *Object // 使用指针类型的Object
		var tmpDataLen int

		if height > 1 {
			// 递归处理下一层数据
			tmpObj, tmpDataLen = dfsHandleFile(height-1, node, store, start, h)
		} else {
			// 处理当前层数据
			end := start + ChunkSize
			if end > len(node.Bytes()) {
				end = len(node.Bytes())
			}
			data := node.Bytes()[start:end]
			// 将数据存储到 KVStore
			blobObj := &Object{ // 使用指针类型的Object
				Links: nil,
				Data:  data,
			}
			putObjInStore(blobObj, store, h)
			// 更新 obj 中的 Links 和 Data
			jsonMarshal, _ := json.Marshal(blobObj)
			obj.Links = append(obj.Links, &Link{ // 存储Link指针
				Hash: computeHash(*blobObj, jsonMarshal, h),
				Size: len(data),
			})

			obj.Data = append(obj.Data, []byte(BLOB)...)
			tmpDataLen = len(data)
			start += ChunkSize
		}

		lenData += tmpDataLen
		jsonMarshal, _ := json.Marshal(tmpObj)
		obj.Links = append(obj.Links, &Link{ // 存储Link指针
			Hash: computeHash(*tmpObj, jsonMarshal, h),
			Size: tmpDataLen,
		})
		obj.Data = append(obj.Data, []byte(LINK)...)

		if start >= len(node.Bytes()) {
			break
		}
	}

	// 将处理好的对象存储到 KVStore
	putObjInStore(obj, store, h)
	return obj, lenData
}

func putObjInStore(obj *Object, store KVStore, h hash.Hash) {
	value, err := json.Marshal(obj)
	if err != nil {
		fmt.Println("json.Marshal err:", err)
		return
	}
	hash := computeHash(*obj, value, h)
	store.Put(hash, value)
}
