package store_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/ecnepsnai/store"
)

var tmpDir string

func testSetup() {
	tmp, err := ioutil.TempDir("", "store")
	if err != nil {
		panic(err)
	}
	tmpDir = tmp
}

func testTeardown() {
	os.RemoveAll(tmpDir)
}

func TestMain(m *testing.M) {
	testSetup()
	retCode := m.Run()
	testTeardown()
	os.Exit(retCode)
}

func TestNew(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestNew", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()
}

func TestNewExtension(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestNewExtension", &store.Options{
		Extension: ".dat",
	})
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	store.Close()

	if _, err := os.Stat(path.Join(tmpDir, "TestNewExtension.dat")); err != nil {
		t.Fatalf("Error stating file: %s", err.Error())
	}
}

func TestNewMode(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestNewMode", &store.Options{
		Mode: 0600,
	})
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	store.Close()

	info, err := os.Stat(path.Join(tmpDir, "TestNewMode.db"))
	if err != nil {
		t.Fatalf("Error stating file: %s", err.Error())
	}
	if info.Mode() != 0600 {
		t.Fatalf("Incorrect file mode. Expected %d got %d", 0600, info.Mode())
	}
}

func TestWrite(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestWrite", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	if err := store.Write("hello", []byte("world")); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestGet", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	key := "hello"
	value := "world"

	if err := store.Write(key, []byte(value)); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	result := store.Get(key)
	if result == nil {
		t.Fatalf("No value returned for key")
	}
	if string(result) != value {
		t.Fatalf("Incorrect value returned for key")
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestUpdate", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	key := "hello"
	value := "world"

	if err := store.Write(key, []byte(value)); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	result := store.Get(key)
	if result == nil {
		t.Fatalf("No value returned for key")
	}
	if string(result) != value {
		t.Fatalf("Incorrect value returned for key")
	}

	value = "new"

	if err := store.Write(key, []byte(value)); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	result = store.Get(key)
	if result == nil {
		t.Fatalf("No value returned for key")
	}
	if string(result) != value {
		t.Fatalf("Incorrect value returned for key")
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestDelete", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	key := "hello"
	value := "world"

	if err := store.Write(key, []byte(value)); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	result := store.Get(key)
	if result == nil {
		t.Fatalf("No value returned for key")
	}
	if string(result) != value {
		t.Fatalf("Incorrect value returned for key")
	}

	if err := store.Delete(key); err != nil {
		t.Fatalf("Error deleting object: %s", err.Error())
	}

	result = store.Get(key)
	if result != nil {
		t.Fatalf("Unexpected value for deleted key")
	}
}

func TestCount(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestCount", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	if err := store.Write("hello", []byte("world")); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	if store.Count() != 1 {
		t.Fatalf("Incorrect object count returned")
	}
}

func TestForeach(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestForeach", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	if err := store.Write("hello", []byte("world")); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	count := 0
	store.ForEach(func(key string, idx int, value []byte) error {
		count++
		return nil
	})

	if count != 1 {
		t.Fatalf("Incorrect object count returned")
	}
}

func TestForeachError(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestForeachError", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	if err := store.Write("hello", []byte("world")); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	err = store.ForEach(func(key string, idx int, value []byte) error {
		return fmt.Errorf("boo")
	})
	if err == nil {
		t.Fatalf("No error seen when one expected")
	}
}

func TestTruncate(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestTruncate", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	if err := store.Write("hello", []byte("world")); err != nil {
		t.Fatalf("Error writing value: %s", err.Error())
	}

	if store.Count() != 1 {
		t.Fatalf("Incorrect object count returned")
	}

	if err := store.Truncate(); err != nil {
		t.Fatalf("Error truncating table: %s", err.Error())
	}

	if store.Count() != 0 {
		t.Fatalf("Incorrect object count returned")
	}
}

func TestCopyTo(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestCopyTo", nil)
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	var b bytes.Buffer

	if err := store.CopyTo(&b); err != nil {
		t.Fatalf("Error copying store to writer: %s", err.Error())
	}
}

func TestBackupTo(t *testing.T) {
	t.Parallel()

	store, err := store.New(tmpDir, "TestBackupTo", &store.Options{
		Mode: 0600,
	})
	if err != nil {
		t.Fatalf("Error opening store: %s", err.Error())
	}
	defer store.Close()

	backupPath := path.Join(tmpDir, "store.backup")
	if err := store.BackupTo(backupPath); err != nil {
		t.Fatalf("Error copying store to file: %s", err.Error())
	}

	info, err := os.Stat(backupPath)
	if err != nil {
		t.Fatalf("Error stating file: %s", err.Error())
	}
	if info.Mode() != 0600 {
		t.Fatalf("Incorrect file mode. Expected %d got %d", 0600, info.Mode())
	}
}
