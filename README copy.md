# Obvious `env` helper methods

[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE-OF-CONDUCT.md)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
![GitHub release](https://img.shields.io/github/release/go-obvious/env.svg)


Simple environment variable helper library

## How to Use


### Installation

```sh
go get github.com/go-obvious/env
```

### Example Usage

```go
package main

import (
    "fmt"
    "github.com/go-obvious/env"
)

func main() {
    value := env.GetOr("MY_VARIABLE", "nothing")
    fmt.Println("Got:", value)
}
```
