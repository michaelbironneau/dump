package dump

import (
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"
	"github.com/labstack/gommon/log"
)

func TestIndex(t *testing.T) {
	defer func() {
		os.RemoveAll("./tests")
	}()
	logger := log.New("test")
	logger.SetLevel(log.DEBUG)
	i := newIndex(&IndexOpts{
		MaxSegmentIdlePeriod: time.Second,      //so everything is purged on manual cleanup
		IndexCleanupInterval: time.Hour,        //so cleanup never actually runs automatically
		TimeToCompaction:     time.Microsecond, //so everything is compacted at once
		RetentionPeriod: time.Microsecond,
		Dir:                  "./tests",
		Logger: logger,
	})
	Convey("Given an index", t, func() {
		Convey("It should say when a segment definitely doesn't exist", func() {
			So(i.Exists("my/key"), ShouldBeFalse)
		})
		Convey("It should read and write to leafs correctly", func() {
			So(i.Append("my/key", []byte("hello world")), ShouldBeNil)
			So(i.Exists("my/key"), ShouldBeTrue)
			err := i.Read("my/key", func(r io.Reader) {
				b, err := ioutil.ReadAll(r)
				So(err, ShouldBeNil)
				So(string(b), ShouldEqual, "hello world")
			})
			So(err, ShouldBeNil)
		})
		Convey("It should behave well when trying to read non-existing segment", func() {
			err := i.Read("no-such-thing", func(r io.Reader) {
				b, err := ioutil.ReadAll(r)
				So(err, ShouldBeNil)
				So(b, ShouldHaveLength, 0)
			})
			So(err, ShouldBeNil)
		})
		Convey("It should clean up and compact correctly", func() {
			time.Sleep(time.Millisecond * 2000)
			i.cleanup()
			So(i.leafs, ShouldBeEmpty)
			err := i.Compact()
			So(err, ShouldBeNil)
			_, err = os.Stat("./tests/my/key.gz")
			So(err, ShouldBeNil)
			err = i.Read("my/key", func(r io.Reader) {
				b, err := ioutil.ReadAll(r)
				So(err, ShouldBeNil)
				So(string(b), ShouldEqual, "hello world")
			})
			So(err, ShouldBeNil)
		})
		Convey("It should apply the retention period correctly", func() {
			time.Sleep(time.Millisecond*10)
			err := i.ApplyRetentionPolicy()
			So(err, ShouldBeNil)
			So(i.leafs, ShouldBeEmpty)
			So(i.Exists("my/key"), ShouldBeFalse)
			_, err = os.Stat("./tests/my/key.gz")
			So(os.IsNotExist(err), ShouldBeTrue)
		})
	})
}

func BenchmarkWrite(b *testing.B){
	b.StopTimer()
	s, err := NewSegment("./benchmark")
	if err != nil {
		panic(err)
	}
	appendItem := "{\"time\":12345678, \"value\": 123.123, \"code\": \"l123\", \"variable\": \"asdfasdf\"}"
	b.StartTimer()
	for i := 0; i< b.N; i++ {
		err := s.Append([]byte(appendItem))
		if err != nil {
			panic(err)
		}
	}
	s.Close()
	os.Remove("./benchmark.dat")
}

func BenchmarkRead(b *testing.B){
	b.StopTimer()
	s, err := NewSegment("./benchmark")
	if err != nil {
		panic(err)
	}
	appendItem := "{\"time\":12345678, \"value\": 123.123, \"code\": \"l123\", \"variable\": \"asdfasdf\"}"
	b.StartTimer()
	for i := 0; i<1000; i++ {
		if err := s.Append([]byte(appendItem)); err != nil {
			panic(err)
		}
	}
	for i := 0; i< b.N; i++ {
		err = s.Read(func(reader io.Reader) {
			_, errI := ioutil.ReadAll(reader)
			if errI != nil {
				panic(errI)
			}
		})
	}
	s.Close()
	os.Remove("./benchmark.dat")
}

func BenchmarkCompressedRead(b *testing.B){
	b.StopTimer()
	s, err := NewSegment("./benchmark")
	if err != nil {
		panic(err)
	}
	appendItem := "{\"time\":12345678, \"value\": 123.123, \"code\": \"l123\", \"variable\": \"asdfasdf\"}"
	for i := 0; i<1000; i++ {
		if err := s.Append([]byte(appendItem)); err != nil {
			panic(err)
		}
	}
	err = s.Compress()
	if err != nil {
		panic(err)
	}
	s, err = NewSegment("./benchmark")
	if err != nil {
		panic(err)
	}
	b.StartTimer()
	for i := 0; i< b.N; i++ {
		err = s.Read(func(reader io.Reader) {
			_, errI := ioutil.ReadAll(reader)
			if errI != nil {
				panic(errI)
			}
		})
	}
	s.Close()
	os.Remove("./benchmark.gz")
}