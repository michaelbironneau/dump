# Dump

[![Go Report Card](http://goreportcard.com/badge/github.com/michaelbironneau/dump)](https://goreportcard.com/report/github.com/michaelbironneau/dump)
[![Build Status](https://travis-ci.org/michaelbironneau/dump.svg?branch=master)](https://travis-ci.org/michaelbironneau/dump/)
[![](https://godoc.org/github.com/michaelbironneau/dump?status.svg)](http://godoc.org/github.com/michaelbironneau/dump)

Dump is a simple append-key-value store that uses the filesystem as an index.

It has some interesting features that may or may not work for you:

1. All operations are either an append or a read. There is no delete or overwrite operation.
2. There is an existence operation `Exists()` that will tell you if a key *may* exist or definitely does not.
3. At the current time, there may only be one appender and/or one reader at a time per key. This may change in the future.
4. The value of each key is stored in a separate file. If you have a lot of keys, you will need to increase the max number of open files in your system. There is an eviction policy for file handles that may solve this problem for you.
5. Values are not compressed until the `TimeToCompaction` is reached. At that point, the value becomes immutable. The meaning of `TimeToCompaction` is that the file has not been accessed (for reading or writing) for this duration. This may change to the last modified time.

## Example

```go
i := New(&IndexOpts{})

i.Append("key", []byte("hello world"))
fmt.Println(i.Exists("key")) // true
i.Read("key", func(r io.Reader) {
				b, _ := ioutil.ReadAll(r)
				fmt.Println(string(b)) // hello world
})
```