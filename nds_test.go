package nds_test

import (
	"encoding/hex"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/yoavfeld/nds"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"cloud.google.com/go/datastore"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
)

func init() {
	if err := nds.InitNDS(context.Background(), memcacheAddr, projectID); err != nil {
		log.Printf("Could not init nds: %v", err)
	}
}

func NewContext(t *testing.T) (context.Context, func()) {
	c := context.Background()
	closeFunc := func(){}
	return c, closeFunc
}

func TestPutGetDelete(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int
	}

	// Check we set memcahce, put datastore and delete memcache.
	seq := make(chan string, 3)
	nds.SetMemcacheSetMulti(func(c context.Context,
		items []*memcache.Item) error {
		seq <- "nds.McClient.SetMulti"
		return nds.McClient.SetMulti(c, items)
	})
	nds.SetDatastorePutMulti(func(c context.Context,
		keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error) {
		seq <- "nds.DsClient.PutMulti"
		return nds.DsClient.PutMulti(c, keys, vals)
	})
	nds.SetMemcacheDeleteMulti(func(c context.Context,
		keys []string) error {
		seq <- "nds.McClient.DeleteMulti"
		close(seq)
		return nds.McClient.DeleteMulti(c, keys)
	})

	incompleteKey := datastore.IncompleteKey("Entity", nil)
	key, err := nds.Put(c, incompleteKey, &testEntity{43})
	if err != nil {
		t.Fatal(err)
	}

	nds.SetMemcacheSetMulti(nds.McClient.SetMulti)
	nds.SetDatastorePutMulti(nds.DsClient.PutMulti)
	nds.SetMemcacheDeleteMulti(nds.McClient.DeleteMulti)

	if s := <-seq; s != "nds.McClient.SetMulti" {
		t.Fatal("nds.McClient.SetMulti not", s)
	}
	if s := <-seq; s != "nds.DsClient.PutMulti" {
		t.Fatal("nds.DsClient.PutMulti not", s)
	}
	if s := <-seq; s != "nds.McClient.DeleteMulti" {
		t.Fatal("nds.McClient.DeleteMulti not", s)
	}
	// Check chan is closed.
	<-seq

	if key.Incomplete() {
		t.Fatal("Key is incomplete")
	}

	te := &testEntity{}
	if err := nds.Get(c, key, te); err != nil {
		t.Fatal(err)
	}

	if te.IntVal != 43 {
		t.Fatal("te.Val != 43", te.IntVal)
	}

	// Get from cache.
	te = &testEntity{}
	if err := nds.Get(c, key, te); err != nil {
		t.Fatal(err)
	}

	if te.IntVal != 43 {
		t.Fatal("te.Val != 43", te.IntVal)
	}

	// Change value.
	if _, err := nds.Put(c, key, &testEntity{64}); err != nil {
		t.Fatal(err)
	}

	// Get from cache.
	te = &testEntity{}
	if err := nds.Get(c, key, te); err != nil {
		t.Fatal(err)
	}

	if te.IntVal != 64 {
		t.Fatal("te.Val != 64", te.IntVal)
	}

	if err := nds.Delete(c, key); err != nil {
		t.Fatal(err)
	}

	if err := nds.Get(c, key, &testEntity{}); err != datastore.ErrNoSuchEntity {
		t.Fatal("expected datastore.ErrNoSuchEntity")
	}
}

func TestInterfaces(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		Val int
	}

	incompleteKey := datastore.IncompleteKey("Entity", nil)
	incompleteKeys := []*datastore.Key{incompleteKey}
	entities := []interface{}{&testEntity{43}}
	keys, err := nds.PutMulti(c, incompleteKeys, entities)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatal("len(keys) != 1")
	}

	if keys[0].Incomplete() {
		t.Fatal("Key is incomplete")
	}

	entities = []interface{}{&testEntity{}}
	if err := nds.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	if entities[0].(*testEntity).Val != 43 {
		t.Fatal("te.Val != 43")
	}

	// Get from cache.
	entities = []interface{}{&testEntity{}}
	if err := nds.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	if entities[0].(*testEntity).Val != 43 {
		t.Fatal("te.Val != 43")
	}

	// Change value.
	entities = []interface{}{&testEntity{64}}
	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	// Get from nds with struct.
	entities = []interface{}{&testEntity{}}
	if err := nds.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}

	if entities[0].(*testEntity).Val != 64 {
		t.Fatal("te.Val != 64")
	}

	if err := nds.DeleteMulti(c, keys); err != nil {
		t.Fatal(err)
	}

	entities = []interface{}{testEntity{}}
	err = nds.GetMulti(c, keys, entities)
	if me, ok := err.(datastore.MultiError); ok {

		if len(me) != 1 {
			t.Fatal("expected 1 datastore.MultiError")
		}
		if me[0] != datastore.ErrNoSuchEntity {
			t.Fatal("expected datastore.ErrNoSuchEntity")
		}
	} else {
		t.Fatal("expected datastore.ErrNoSuchEntity", err)
	}
}

func TestGetMultiNoSuchEntity(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		Val int
	}

	// Test no such entity.
	for _, count := range []int{999, 1000, 1001} {

		keys := []*datastore.Key{}
		entities := []*testEntity{}
		for i := 0; i < count; i++ {
			keys = append(keys,
				datastore.NameKey("Test", strconv.Itoa(i), nil))
			entities = append(entities, &testEntity{})
		}

		err := nds.GetMulti(c, keys, entities)
		if me, ok := err.(datastore.MultiError); ok {
			if len(me) != count {
				t.Fatal("multi error length incorrect")
			}
			for _, e := range me {
				if e != datastore.ErrNoSuchEntity {
					t.Fatal("expecting datastore.ErrNoSuchEntity but got", e)
				}
			}
		}
	}
}

func TestGetMultiNoErrors(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		Val int
	}

	for _, count := range []int{999, 1000, 1001} {

		// Create entities.
		keys := []*datastore.Key{}
		entities := []*testEntity{}
		for i := 0; i < count; i++ {
			key := datastore.NameKey("Test", strconv.Itoa(i), nil)
			keys = append(keys, key)
			entities = append(entities, &testEntity{i})
		}

		// Save entities.
		if _, err := nds.PutMulti(c, keys, entities); err != nil {
			t.Fatal(err)
		}

		respEntities := []testEntity{}
		for range keys {
			respEntities = append(respEntities, testEntity{})
		}

		if err := nds.GetMulti(c, keys, respEntities); err != nil {
			t.Fatal(err)
		}

		// Check respEntities are in order.
		for i, re := range respEntities {
			if re.Val != entities[i].Val {
				t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
					entities[i].Val)
			}
		}
	}
}

func TestGetMultiErrorMix(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		Val int
	}

	for _, count := range []int{999, 1000, 1001} {

		// Create entities.
		keys := []*datastore.Key{}
		entities := []testEntity{}
		for i := 0; i < count; i++ {
			key := datastore.NameKey("Test", strconv.Itoa(i), nil)
			keys = append(keys, key)
			entities = append(entities, testEntity{i})
		}

		// Save every other entity.
		putKeys := []*datastore.Key{}
		putEntities := []testEntity{}
		for i, key := range keys {
			if i%2 == 0 {
				putKeys = append(putKeys, key)
				putEntities = append(putEntities, entities[i])
			}
		}

		if _, err := nds.PutMulti(c, putKeys, putEntities); err != nil {
			t.Fatal(err)
		}

		respEntities := make([]testEntity, len(keys))
		err := nds.GetMulti(c, keys, respEntities)
		if err == nil {
			t.Fatal("should be errors")
		}

		if me, ok := err.(datastore.MultiError); !ok {
			t.Fatal("not datastore.MultiError")
		} else if len(me) != len(keys) {
			t.Fatal("incorrect length datastore.MultiError")
		}

		// Check respEntities are in order.
		for i, re := range respEntities {
			if i%2 == 0 {
				if re.Val != entities[i].Val {
					t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
						entities[i].Val)
				}
			} else if me, ok := err.(datastore.MultiError); ok {
				if me[i] != datastore.ErrNoSuchEntity {
					t.Fatalf("incorrect error %+v, index %d, of %d",
						me, i, count)
				}
			} else {
				t.Fatalf("incorrect error, index %d", i)
			}
		}
	}
}

func TestMultiCache(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		Val int
	}
	const entityCount = 88

	// Create entities.
	keys := []*datastore.Key{}
	entities := []testEntity{}
	for i := 0; i < entityCount; i++ {
		key := datastore.NameKey("Test", strconv.Itoa(i), nil)
		keys = append(keys, key)
		entities = append(entities, testEntity{i})
	}

	// Save every other entity.
	putKeys := []*datastore.Key{}
	putEntities := []testEntity{}
	for i, key := range keys {
		if i%2 == 0 {
			putKeys = append(putKeys, key)
			putEntities = append(putEntities, entities[i])
		}
	}
	if keys, err := nds.PutMulti(c, putKeys, putEntities); err != nil {
		t.Fatal(err)
	} else if len(keys) != len(putKeys) {
		t.Fatal("incorrect key len")
	}

	// Get from nds.
	respEntities := make([]testEntity, len(keys))
	err := nds.GetMulti(c, keys, respEntities)
	if err == nil {
		t.Fatal("should be errors")
	}

	me, ok := err.(datastore.MultiError)
	if !ok {
		t.Fatalf("not an datastore.MultiError: %+T, %s", err, err)
	}

	// Check respEntities are in order.
	for i, re := range respEntities {
		if i%2 == 0 {
			if re.Val != entities[i].Val {
				t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
					entities[i].Val)
			}
			if me[i] != nil {
				t.Fatalf("should be nil error: %s", me[i])
			}
		} else {
			if re.Val != 0 {
				t.Fatal("entity not zeroed")
			}
			if me[i] != datastore.ErrNoSuchEntity {
				t.Fatalf("incorrect error %+v, index %d, of %d",
					me, i, entityCount)
			}
		}
	}

	// Get from local cache.
	respEntities = make([]testEntity, len(keys))
	err = nds.GetMulti(c, keys, respEntities)
	if err == nil {
		t.Fatal("should be errors")
	}

	me, ok = err.(datastore.MultiError)
	if !ok {
		t.Fatalf("not an datastore.MultiError: %s", err)
	}

	// Check respEntities are in order.
	for i, re := range respEntities {
		if i%2 == 0 {
			if re.Val != entities[i].Val {
				t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
					entities[i].Val)
			}
			if me[i] != nil {
				t.Fatal("should be nil error")
			}
		} else {
			if re.Val != 0 {
				t.Fatal("entity not zeroed")
			}
			if me[i] != datastore.ErrNoSuchEntity {
				t.Fatalf("incorrect error %+v, index %d, of %d",
					me, i, entityCount)
			}
		}
	}

	// Get from memcache.
	respEntities = make([]testEntity, len(keys))
	err = nds.GetMulti(c, keys, respEntities)
	if err == nil {
		t.Fatal("should be errors")
	}

	me, ok = err.(datastore.MultiError)
	if !ok {
		t.Fatalf("not an datastore.MultiError: %+T", me)
	}

	// Check respEntities are in order.
	for i, re := range respEntities {
		if i%2 == 0 {
			if re.Val != entities[i].Val {
				t.Fatalf("respEntities in wrong order, %d vs %d", re.Val,
					entities[i].Val)
			}
			if me[i] != nil {
				t.Fatal("should be nil error")
			}
		} else {
			if re.Val != 0 {
				t.Fatal("entity not zeroed")
			}
			if me[i] != datastore.ErrNoSuchEntity {
				t.Fatalf("incorrect error %+v, index %d, of %d",
					me, i, entityCount)
			}
		}
	}
}

func TestRunInTransaction(t *testing.T) {
	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		Val int
	}

	key := datastore.IDKey("Entity", 3, nil)
	keys := []*datastore.Key{key}
	entity := testEntity{42}
	entities := []testEntity{entity}

	if _, err := nds.PutMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}
	// TODO: enable
	//err := nds.RunInTransaction(c, func(tc context.Context) error {
	//	entities := make([]testEntity, 1, 1)
	//	if err := nds.GetMulti(tc, keys, entities); err != nil {
	//		t.Fatal(err)
	//	}
	//	entity := entities[0]
	//
	//	if entity.Val != 42 {
	//		t.Fatalf("entity.Val != 42: %d", entity.Val)
	//	}
	//
	//	entities[0].Val = 43
	//
	//	putKeys, err := nds.PutMulti(tc, keys, entities)
	//	if err != nil {
	//		t.Fatal(err)
	//	} else if len(putKeys) != 1 {
	//		t.Fatal("putKeys should be len 1")
	//	} else if !putKeys[0].Equal(key) {
	//		t.Fatal("keys not equal")
	//	}
	//	return nil
	//
	//}, nil)
	//if err != nil {
	//	t.Fatal(err)
	//}

	entities = make([]testEntity, 1, 1)
	if err := nds.GetMulti(c, keys, entities); err != nil {
		t.Fatal(err)
	}
	entity = entities[0]
	if entity.Val != 43 {
		t.Fatalf("entity.Val != 43: %d", entity.Val)
	}
}

func TestMarshalUnmarshalPropertyList(t *testing.T) {

	timeVal := time.Now()
	timeProp := datastore.Property{Name: "Time",
		Value: timeVal, NoIndex: false}

	byteStringVal := []byte{0x23}
	byteStringProp := datastore.Property{Name: "ByteString",
		Value: byteStringVal, NoIndex: false}

	keyVal := datastore.NameKey("Entity", "stringID",  nil)
	keyProp := datastore.Property{Name: "Key",
		Value: keyVal, NoIndex: false}

	blobKeyVal := appengine.BlobKey("blobkey")
	blobKeyProp := datastore.Property{Name: "BlobKey",
		Value: blobKeyVal, NoIndex: false}

	geoPointVal := appengine.GeoPoint{1, 2}
	geoPointProp := datastore.Property{Name: "GeoPoint",
		Value: geoPointVal, NoIndex: false}

	pl := datastore.PropertyList{
		timeProp,
		byteStringProp,
		keyProp,
		blobKeyProp,
		geoPointProp,
	}
	data, err := nds.MarshalPropertyList(pl)
	if err != nil {
		t.Fatal(err)
	}

	testEntity := &struct {
		Time       time.Time
		ByteString []byte
		Key        *datastore.Key
		BlobKey    appengine.BlobKey
		GeoPoint   appengine.GeoPoint
	}{}

	pl = datastore.PropertyList{}
	if err := nds.UnmarshalPropertyList(data, &pl); err != nil {
		t.Fatal(err)
	}
	if err := nds.SetValue(reflect.ValueOf(testEntity), pl); err != nil {
		t.Fatal(err)
	}

	if !testEntity.Time.Equal(timeVal) {
		t.Fatal("timeVal not equal")
	}

	if string(testEntity.ByteString) != string(byteStringVal) {
		t.Fatal("byteStringVal not equal")
	}

	if !testEntity.Key.Equal(keyVal) {
		t.Fatal("keyVal not equal")
	}

	if testEntity.BlobKey != blobKeyVal {
		t.Fatal("blobKeyVal not equal")
	}

	if !reflect.DeepEqual(testEntity.GeoPoint, geoPointVal) {
		t.Fatal("geoPointVal not equal")
	}
}

func TestMartialPropertyListError(t *testing.T) {

	type testEntity struct {
		IntVal int
	}

	pl := datastore.PropertyList{
		datastore.Property{"Prop", &testEntity{3}, false},
	}
	if _, err := nds.MarshalPropertyList(pl); err == nil {
		t.Fatal("expected error")
	}
}

func randHexString(length int) string {
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = byte(rand.Int())
	}
	return hex.EncodeToString(bytes)
}

func TestCreateMemcacheKey(t *testing.T) {

	// Check keys are hashed over nds.MemcacheMaxKeySize.
	maxKeySize := nds.MemcacheMaxKeySize
	key := datastore.NameKey("TestEntity",
		randHexString(maxKeySize+10),  nil)

	memcacheKey := nds.CreateMemcacheKey(key)
	if len(memcacheKey) > maxKeySize {
		t.Fatal("incorrect memcache key size")
	}
}

func TestMemcacheNamespace(t *testing.T) {

	c, closeFunc := NewContext(t)
	defer closeFunc()

	type testEntity struct {
		IntVal int
	}

	// Illegal namespace chars.
	nds.SetMemcacheNamespace("£££")

	key := datastore.IDKey("Entity", 1, nil)
	if err := nds.Get(c, key, &testEntity{}); err == nil {
		t.Fatal("expected namespace error")
	}

	if _, err := nds.Put(c, key, &testEntity{}); err == nil {
		t.Fatal("expected namespace error")
	}

	if err := nds.Delete(c, key); err == nil {
		t.Fatal("expected namespace error")
	}

	//TODO: enable
	//if err := nds.RunInTransaction(c, func(tc context.Context) error {
	//	return nil
	//}, nil); err == nil {
	//	t.Fatal("expected namespace error")
	//}

	nds.SetMemcacheNamespace("")
}
