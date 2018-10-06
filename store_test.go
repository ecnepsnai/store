package store

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/ecnepsnai/console"
)

var s *Store
var dir *string

func setup() error {
	tmpDir, err := ioutil.TempDir("", "store")
	if err != nil {
		return err
	}
	console, _ := console.New(console.Config{
		PrintLevel: console.LevelDebug,
	})
	st, err := New(tmpDir, "store", console)
	if err != nil {
		return err
	}

	s = st
	dir = &tmpDir
	return nil
}

func teardown() {
	s.Close()
	os.RemoveAll(*dir)
}

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		fmt.Printf("Error opening store: %s\n", err.Error())
		os.Exit(1)
	}

	retCode := m.Run()
	teardown()
	os.Exit(retCode)
}

func TestReadWrite(t *testing.T) {
	testString := "hello world"
	data := []byte(testString)

	if err := s.Write("key1", data); err != nil {
		t.Errorf("Error writing key: %s", err.Error())
		t.Fail()
	}

	retData := s.Get("key1")
	if string(retData) != testString {
		t.Errorf("Return data was incorrect. Expected '%s' got '%s'", testString, retData)
		t.Fail()
	}
}

func TestMissingKey(t *testing.T) {
	data := s.Get("doesn't exist!")
	if data != nil {
		t.Errorf("Return data should be nil, got '%x'", data)
		t.Fail()
	}
}

func TestCount(t *testing.T) {
	count := s.Count()
	if count != 1 {
		t.Errorf("Item count was not expected. Got %d, expected 1", count)
		t.Fail()
	}
}

func TestForEach(t *testing.T) {
	i := 0
	err := s.ForEach(func(key string, idx int, value []byte) error {
		if key == "" {
			t.Errorf("key is empty")
			t.Fail()
		}
		if idx < 0 {
			t.Errorf("item index is less than 0")
			t.Fail()
		}
		if value == nil {
			t.Errorf("value is nil")
			t.Fail()
		}

		i++

		return nil
	})

	if i == 0 {
		t.Errorf("foreach cb never invoked")
		t.Fail()
	}

	if err != nil {
		t.Errorf("Error performing foreach: %s", err.Error())
		t.Fail()
	}
}

func TestDelete(t *testing.T) {
	key := "doomed"

	if err := s.Write(key, []byte("DOOMED!")); err != nil {
		t.Errorf("Error writing key: %s", err.Error())
		t.Fail()
	}

	if err := s.Delete(key); err != nil {
		t.Errorf("Error deleting key: %s", err.Error())
		t.Fail()
	}

	data := s.Get(key)
	if data != nil {
		t.Errorf("Key '%s' should not exist, has value '%s'", key, data)
		t.Fail()
	}
}

func TestTruncate(t *testing.T) {
	if err := s.Truncate(); err != nil {
		t.Errorf("Error truncating store: %s", err.Error())
		t.Fail()
	}

	data := s.Get("key1")
	if data != nil {
		t.Errorf("Key should not exist, has value '%s'", data)
		t.Fail()
	}
}

func TestBackup(t *testing.T) {
	f, err := os.Create(path.Join(*dir, "writer"))
	if err != nil {
		t.Errorf("Error creating writer: %s", err.Error())
		t.Fail()
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	if err = s.CopyTo(w); err != nil {
		t.Errorf("Error copying store data: %s", err.Error())
		t.Fail()
	}

	if err = s.BackupTo(path.Join(*dir, "backup")); err != nil {
		t.Errorf("Error backing up store: %s", err.Error())
		t.Fail()
	}
}
