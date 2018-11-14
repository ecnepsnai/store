# store
A go package to make working with BoltDB easier

# Installation

```
go get github.com/ecnepsnai/store
```

# Usage

```golang
package main

import (
    "github.com/ecnepsnai/store"
)

func main() {
    store, err := New("data", "users")
    if err != nil {
        panic(err.Error())
    }
    // Make sure to close your store when you're finished
    defer store.Close()

    // Getting an object
    data := store.Get("ecnepsnai")
    if data != nil {
        // Do something with your data
    }

    // Setting an object
    if err = store.Write("ecnepsnai", []byte("is awesome")); err != nil {
        panic(err.Error())
    }

    // Deleting an object
    if err = store.Delete("test"); err != nil {
        panic(err.Error())
    }

    // Iterating over all objects
    store.ForEach(func(key []byte, idx int, value []byte) error {
        username := string(key)
        // Do something with each object
        return nil
    })
}
```