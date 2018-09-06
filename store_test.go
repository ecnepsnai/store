// Package store makes working with BoltDB easier
package store

import (
	"bufio"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/ecnepsnai/console"
)

func TestStore(t *testing.T) {
	Console := console.NewMemory(console.LevelDebug)
	dir, err := ioutil.TempDir("", "store_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	store, err := New(dir, "user", &Console)
	if err != nil {
		t.Errorf("%s", err.Error())
		t.Fail()
	}
	defer store.Close()

	if err = store.Write("test", []byte("123")); err != nil {
		t.Errorf("%s", err.Error())
		t.Fail()
	}

	data := store.Get("test")
	if string(data) != "123" {
		t.Errorf("Returned data was different than what we wrote")
		t.Fail()
	}

	count := 0
	store.ForEach(func(key []byte, idx int, value []byte) error {
		if string(key) == "test" && string(value) == "123" {
			count++
		}
		return nil
	})
	if count != 1 {
		t.Errorf("Returned data was different than what we wrote")
		t.Fail()
	}

	if store.Count() != 1 {
		t.Errorf("Returned data was different than what we wrote")
		t.Fail()
	}

	if err = store.Delete("test"); err != nil {
		t.Errorf("%s", err.Error())
		t.Fail()
	}

	f, err := os.Create(path.Join(dir, "writer"))
	if err != nil {
		t.Errorf("%s", err.Error())
		t.Fail()
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	if err = store.CopyTo(w); err != nil {
		t.Errorf("%s", err.Error())
		t.Fail()
	}

	if err = store.BackupTo(path.Join(dir, "backup")); err != nil {
		t.Errorf("%s", err.Error())
		t.Fail()
	}
}
