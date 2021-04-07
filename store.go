/*
Package store provides a fast and efficient file-based key-value store.
*/
package store

import (
	"io"
	"io/fs"
	"path"
	"time"

	"github.com/ecnepsnai/logtic"
	"go.etcd.io/bbolt"
)

type bucket struct {
	name []byte
}

// Store describes a store object
type Store struct {
	Name    string
	Options Options
	path    string
	bucket  bucket
	client  *bbolt.DB
	log     *logtic.Source
}

// Options describes options for creating a new store
type Options struct {
	Mode      fs.FileMode // Defaults to 0644
	Extension string      // Defaults to .db
}

// New will create or open a store with the given store name at the specified data directory.
// Options may be nil and the defaults will be used.
func New(dataDir string, storeName string, options *Options) (*Store, error) {
	o := Options{
		Mode:      0644,
		Extension: ".db",
	}
	if options != nil {
		if options.Extension != "" {
			o.Extension = options.Extension
		}
		if options.Mode > 0 {
			o.Mode = options.Mode
		}
	}

	s := Store{
		path: path.Join(dataDir, storeName+o.Extension),
		Name: storeName,
		bucket: bucket{
			name: []byte(storeName),
		},
		log:     logtic.Connect("store(" + storeName + ")"),
		Options: o,
	}

	client, err := bbolt.Open(s.path, o.Mode, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		s.log.Error("Error opening store '%s': %s", s.path, err.Error())
		return nil, err
	}
	s.client = client
	err = client.Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(s.bucket.name) == nil {
			s.log.Debug("Creating bucket '%s'", s.Name)
			_, txerr := tx.CreateBucketIfNotExists(s.bucket.name)
			return txerr
		}
		return nil
	})
	if err != nil {
		s.log.Error("Error creating bucket '%s': %s", s.Name, err.Error())
		return nil, err
	}

	s.log.Debug("'%s' Opened", s.Name)
	return &s, nil
}

// Close will close the store. This may block if there are any ongoing writes.
func (s *Store) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

// Get will fetch the given key from the store and return its data, or nil if no record was found.
func (s *Store) Get(key string) []byte {
	var value []byte
	s.client.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket.name)
		value = bucket.Get([]byte(key))
		s.log.Debug("Get %s.%s", s.Name, key)
		return nil
	})
	return value
}

// Count will return the number of objects in the store.
func (s *Store) Count() int {
	var count int
	s.client.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket.name)
		count = bucket.Stats().KeyN
		return nil
	})
	return count
}

// ForEach will invoke cb for each object in the store with the key, index, and the value for that object
func (s *Store) ForEach(cb func(key string, idx int, value []byte) error) error {
	return s.client.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket.name)
		s.log.Debug("Foreach %s", s.Name)
		var i = -1
		return bucket.ForEach(func(key []byte, value []byte) error {
			i++
			return cb(string(key), i, value)
		})
	})
}

// Write saves a new object or updates an existing object in the store
func (s *Store) Write(key string, value []byte) error {
	return s.client.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket.name)
		s.log.Debug("Set %s.%s", s.Name, key)
		return bucket.Put([]byte(key), value)
	})
}

// Truncate will remove all keys from the store
func (s *Store) Truncate() error {
	return s.client.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket(s.bucket.name); err != nil {
			return err
		}
		s.log.Debug("Deleting bucket '%s'", s.bucket.name)
		if _, err := tx.CreateBucket(s.bucket.name); err != nil {
			return err
		}
		s.log.Debug("Creating bucket '%s'", s.bucket.name)
		return nil
	})
}

// Delete will delete the object with the specified key from the store. If they key does not exist it does nothing.
func (s *Store) Delete(key string) error {
	return s.client.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket.name)
		s.log.Debug("Delete %s.%s", s.Name, key)
		return bucket.Delete([]byte(key))
	})
}

// CopyTo will make a copy of the store to the specified writer without blocking the store
func (s *Store) CopyTo(writer io.Writer) error {
	return s.client.View(func(tx *bbolt.Tx) error {
		s.log.Debug("Copy %s", s.Name)
		return tx.Copy(writer)
	})
}

// BackupTo will make a copy of the store to the specified file path. The file will have the same mode as used when the
// store was created as specified in the options.
func (s *Store) BackupTo(filePath string) error {
	return s.client.View(func(tx *bbolt.Tx) error {
		s.log.Debug("Backup %s -> %s", s.Name, filePath)
		return tx.CopyFile(filePath, s.Options.Mode)
	})
}
