package nds

import (
	"golang.org/x/net/context"
	"github.com/bradfitz/gomemcache/memcache"
	"cloud.google.com/go/datastore"
)

type memcacheClient struct {
	*memcache.Client
}

func NewMemcache(addr string) *memcacheClient{
	return &memcacheClient{memcache.New(addr)}
}

func (mc *memcacheClient) AddMulti(c context.Context, items []*memcache.Item) error {
	if McClient == nil {
		panic("memcache was not initialized. did you call nds.Init ?")
	}
	multiErr, any := make(datastore.MultiError, len(items)), false
	for i, item := range items {
		if err := mc.Add(item); err != nil {
			multiErr[i] = err
			any = true
		}
	}
	if any {
		return multiErr
	}
	return nil
}
func (mc *memcacheClient) SetMulti(c context.Context, items []*memcache.Item) error {
	if McClient == nil {
		panic("memcache was not initialized. did you call nds.Init ?")
	}
	multiErr, any := make(datastore.MultiError, len(items)), false
	for i, item := range items {
		if err := mc.Set(item); err != nil {
			multiErr[i] = err
			any = true
		}
	}
	if any {
		return multiErr
	}
	return nil
}

func (mc *memcacheClient) GetMulti(c context.Context, keys []string) (map[string]*memcache.Item, error) {
	return McClient.Client.GetMulti(keys)
}

func (mc *memcacheClient) DeleteMulti(c context.Context, keys []string) error {
	if McClient == nil {
		panic("memcache was not initialized. did you call nds.Init ?")
	}
	multiErr, any := make(datastore.MultiError, len(keys)), false
	for i, key := range keys {
		if err := mc.Delete(key); err != nil {
			multiErr[i] = err
			any = true
		}
	}
	if any {
		return multiErr
	}
	return nil
}

func (mc *memcacheClient) CompareAndSwapMulti(c context.Context, items []*memcache.Item) error {
	if McClient == nil {
		panic("memcache was not initialized. did you call nds.Init ?")
	}
	multiErr, any := make(datastore.MultiError, len(items)), false
	for i, item := range items {
		if err := mc.CompareAndSwap(item); err != nil {
			multiErr[i] = err
			any = true
		}
	}
	if any {
		return multiErr
	}
	return nil
}

