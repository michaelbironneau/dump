package dump

import (
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	s, err := NewSegment("./test/key")
	defer func() {
		//cleanup
		s.Close()
		os.RemoveAll("./test")
	}()
	Convey("When creating a new segment", t, func() {
		Convey("It should create the file correctly", func() {
			So(err, ShouldBeNil)
			f, err := os.Stat("./test/key.dat")
			So(err, ShouldBeNil)
			So(f.IsDir(), ShouldBeFalse)
		})
		Convey("It should write to the file correctly", func() {
			err := s.Append([]byte("hello world\n"))
			So(err, ShouldBeNil)
			err = s.Append([]byte("foo"))
			So(err, ShouldBeNil)
		})
		Convey("It should read from the file correctly", func() {
			err := s.Read(func(r io.Reader) {
				b, err := ioutil.ReadAll(r)
				So(err, ShouldBeNil)
				So(string(b), ShouldEqual, "hello world\nfoo")
			})
			So(err, ShouldBeNil)
		})
		Convey("It should compress the file correctly", func() {
			err := s.Compress()
			So(err, ShouldBeNil)
			err = s.Compress()
			So(err, ShouldNotBeNil) //segment is immutable
			f, err := os.Stat("./test/key.gz")
			So(err, ShouldBeNil)
			So(f.IsDir(), ShouldBeFalse)
			//reference to original segment now is invalid - recreate
			s2, err2 := NewSegment("./test/key")
			So(err2, ShouldBeNil)
			err = s2.Read(func(r io.Reader) {
				b, err := ioutil.ReadAll(r)
				So(err, ShouldBeNil)
				So(string(b), ShouldEqual, "hello world\nfoo")
			})
			So(err, ShouldBeNil)
			//try again just to make sure we have Seek'ed back to the start
			err = s2.Read(func(r io.Reader) {
				b, err := ioutil.ReadAll(r)
				So(err, ShouldBeNil)
				So(string(b), ShouldEqual, "hello world\nfoo")
			})
			So(err, ShouldBeNil)
		})
	})
}
