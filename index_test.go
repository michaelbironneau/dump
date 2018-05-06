package dump

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
	"io"
	"io/ioutil"
	"os"
)

func TestIndex(t *testing.T) {
	defer func() {
		os.RemoveAll("./tests")
	}()
	i := NewIndex(&IndexOpts{
		MaxSegmentIdlePeriod: time.Second, //so everything is purged on manual cleanup
		IndexCleanupInterval: time.Hour,   //so cleanup never actually runs automatically
		TimeToCompaction: time.Microsecond, //so everything is compacted at once
		Dir:                  "./tests",
	})
	Convey("Given an index", t, func() {
		Convey("It should say when a segment definitely doesn't exist", func() {
			So(i.Exists("key"), ShouldBeFalse)
		})
		Convey("It should read and write to leafs correctly", func() {
			So(i.Write("key", []byte("hello world")), ShouldBeNil)
			So(i.Exists("key"), ShouldBeTrue)
			err := i.Read("key", func(r io.Reader) {
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
			_, err = os.Stat("./tests/key.gz")
			So(err, ShouldBeNil)
			err = i.Read("key", func(r io.Reader){
				b, err := ioutil.ReadAll(r)
				So(err, ShouldBeNil)
				So(string(b), ShouldEqual, "hello world")
			})
			So(err, ShouldBeNil)
		})
	})
}
