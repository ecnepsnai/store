package store_test

import (
	"fmt"

	"github.com/ecnepsnai/store"
)

func ExampleNew() {
	store, err := store.New("/path/to/your/data/dir", "StoreName")
	if err != nil {
		panic(err)
	}

	// Don't forget to close your store when you're finished (such as when the application exits)
	store.Close()
}

func ExampleStore_Get() {
	var store *store.Store // Assumes you've already opened a store, see store.New() for an example

	value := store.Get("key1")
	if value == nil {
		// No object with key 'key1'
	}
}

func ExampleStore_Count() {
	var store *store.Store // Assumes you've already opened a store, see store.New() for an example

	count := store.Count()
	fmt.Printf("Object: %d\n", count)
}

func ExampleStore_ForEach() {
	var store *store.Store // Assumes you've already opened a store, see store.New() for an example

	count := 0
	store.ForEach(func(key string, idx int, value []byte) error {
		count++
		return nil
	})

	fmt.Printf("Object: %d\n", count)
}

func ExampleStore_Write() {
	var store *store.Store // Assumes you've already opened a store, see store.New() for an example

	if err := store.Write("key1", []byte("value1")); err != nil {
		panic(err)
	}

	if err := store.Write("key1", []byte("value2")); err != nil {
		panic(err)
	}
}

func ExampleStore_Truncate() {
	var store *store.Store // Assumes you've already opened a store, see store.New() for an example

	if err := store.Truncate(); err != nil {
		panic(err)
	}
}

func ExampleStore_Delete() {
	var store *store.Store // Assumes you've already opened a store, see store.New() for an example

	if err := store.Delete("key1"); err != nil {
		panic(err)
	}
}
