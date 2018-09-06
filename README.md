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
    "github.com/ecnepsnai/console"
    "github.com/ecnepsnai/store"
)

func main() {
    Console, err := console.New(logPath, console.LevelDebug)
    if err != nil {
        panic(err.Error())
    }

    store, err := store.New("data", "users", Console)
    if err != nil {
        panic(err.Error())
    }

    store.Write("ecnepsnai", []byte("Is awesome"))
    result, err := store.Get("ecnepsnai")
    // will return nil, nil if no record found
    if err != nil {
        panic(err.Error())
    }
    if reault != nil {
        // Do something with the data
    }
}
```