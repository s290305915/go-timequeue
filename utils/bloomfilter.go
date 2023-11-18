package utils

import (
	"github.com/spaolacci/murmur3"
	"github.com/wangjia184/sortedset"
	"sync"
)

var GlobalBloom *BloomFilter

type BloomFilter struct {
	mLock     sync.RWMutex           // 读写锁，用于并发读取和写入
	bucketNum int                    // 桶数量、也是哈希函数的个数
	ExtHash   []*sortedset.SortedSet // 使用有序集合代替 map，减少内存消耗
}

type Bloom struct {
	Hash   []uint64 // 根据SeedSum64算法计算的哈希值
	Bucket int
	Key    string
}

// NewBloom 根据unicode编码最大值 0x10FFFF，设置空间
func NewBloom(bucketNum int) *BloomFilter {
	// 初始化过滤器
	GlobalBloom = &BloomFilter{
		mLock:     sync.RWMutex{},
		bucketNum: bucketNum,
		ExtHash:   make([]*sortedset.SortedSet, bucketNum),
	}
	for i := 0; i < bucketNum; i++ {
		GlobalBloom.ExtHash[i] = sortedset.New()
	}
	return GlobalBloom
}

func hash(key string) ([]uint64, int) {
	// 做哈希
	runeArr := []rune(key)
	hashes := make([]uint64, len(runeArr))

	bucket := murmur3.Sum32([]byte(key)) % uint32(GlobalBloom.bucketNum)

	for i, r := range runeArr {
		d := uint64(r)
		hashes[i] = d
	}

	return hashes, int(bucket)
}

func GetHashBloom(key string) *Bloom {
	// 根据哈希值判断是否可能存在
	hashes, bucket := hash(key)

	blObj := Bloom{
		Hash:   hashes,
		Bucket: bucket,
		Key:    key,
	}
	return &blObj
}

func (b *BloomFilter) SetBloom(key string) (*Bloom, error) {
	blObj := GetHashBloom(key)

	b.mLock.Lock()
	defer b.mLock.Unlock()

	if b.ExtHash[blObj.Bucket] == nil {
		b.ExtHash[blObj.Bucket] = sortedset.New()
	}
	b.ExtHash[blObj.Bucket].AddOrUpdate(key, 0, blObj)
	return blObj, nil
}

func (b *BloomFilter) ExistsBloom(key string) bool {
	blObj := GetHashBloom(key)

	if b.ExtHash[blObj.Bucket] == nil {
		return false
	}
	scorePair := b.ExtHash[blObj.Bucket].GetByKey(key)
	if scorePair != nil {
		return true
	}
	return false
}

// 根据哈希值进行删除操作
func (b *BloomFilter) RemoveBloom(key string) bool {
	blObj := GetHashBloom(key)
	b.mLock.Lock()
	defer b.mLock.Unlock()

	if b.ExtHash[blObj.Bucket] == nil {
		return false
	}

	b.ExtHash[blObj.Bucket].Remove(key)
	return true
}
