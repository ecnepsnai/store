/*
Package store makes working with BoltDB easier

Creating a store

	store, err := New("data", "users")
	if err != nil {
		panic(err.Error())
	}
	// Make sure to close your store when you're finished
	defer store.Close()

Getting an object

	data := store.Get("ecnepsnai")
	if data != nil {
		// Do something with your data
	}

Setting an object

	if err = store.Write("ecnepsnai", []byte("is awesome")); err != nil {
		panic(err.Error())
	}

Deleting an object

	if err = store.Delete("test"); err != nil {
		panic(err.Error())
	}

Iterating over all objects

	store.ForEach(func(key string, idx int, value []byte) error {
		username := key
		// Do something with each object
		return nil
	})

*/
package store

import (
	"io"
	"time"

	"github.com/boltdb/bolt"
	"github.com/ecnepsnai/logtic"
)

type bucket struct {
	name []byte
}

// Store describes a store object
type Store struct {
	path   string
	Name   string
	bucket bucket
	client *bolt.DB
	log    *logtic.Source
}

// New open a new store. Write the store data in the provided directory
func New(dataDir string, storeName string) (*Store, error) {
	s := Store{
		path: dataDir + "/" + storeName + ".db",
		Name: storeName,
		bucket: bucket{
			name: []byte(storeName),
		},
		log: logtic.Connect("store:" + storeName),
	}

	client, err := bolt.Open(s.path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		s.log.Error("Error opening store '%s': %s", s.path, err.Error())
		return nil, err
	}
	s.client = client
	err = client.Update(func(tx *bolt.Tx) error {
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

// Close close the store
func (s *Store) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

// Get get a specific key from the store
func (s *Store) Get(key string) []byte {
	var value []byte
	s.client.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket.name)
		value = bucket.Get([]byte(key))
		s.log.Debug("Get %s.%s", s.Name, key)
		return nil
	})
	return value
}

// Count get the number of keys in the store
func (s *Store) Count() int {
	var count int
	s.client.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket.name)
		count = bucket.Stats().KeyN
		return nil
	})
	return count
}

// ForEach iterate over each object in the store
func (s *Store) ForEach(cb func(key string, idx int, value []byte) error) error {
	return s.client.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket.name)
		s.log.Debug("Foreach %s", s.Name)
		var i = -1
		return bucket.ForEach(func(key []byte, value []byte) error {
			i++
			return cb(string(key), i, value)
		})
	})
}

// Write write a new object to the store
func (s *Store) Write(key string, value []byte) error {
	return s.client.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket.name)
		s.log.Debug("Set %s.%s", s.Name, key)
		return bucket.Put([]byte(key), value)
	})
}

// Truncate remove all keys from the store
func (s *Store) Truncate() error {
	return s.client.Update(func(tx *bolt.Tx) error {
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

// Delete delete a specific object from the store
func (s *Store) Delete(key string) error {
	return s.client.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.bucket.name)
		s.log.Debug("Delete %s.%s", s.Name, key)
		return bucket.Delete([]byte(key))
	})
}

// CopyTo make a hot copy of the store to the given writer
func (s *Store) CopyTo(writer io.Writer) error {
	return s.client.View(func(tx *bolt.Tx) error {
		s.log.Debug("Copy %s", s.Name)
		return tx.Copy(writer)
	})
}

// BackupTo make a hot backup of the store and save it to the specified file
func (s *Store) BackupTo(file string) error {
	return s.client.View(func(tx *bolt.Tx) error {
		s.log.Debug("Backup %s -> %s", s.Name, file)
		return tx.CopyFile(file, 0644)
	})
}
