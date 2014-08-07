package mailbox

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestConsulNode(t *testing.T) {
	dir, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir)

	id1 := "127.0.0.1:8899"

	cn1, err := NewConsulClusterNode(id1, ":8899", dir)
	if err != nil {
		panic(err)
	}

	defer cn1.Close()

	dir2, err := ioutil.TempDir("", "mailbox")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir2)

	id2 := "127.0.0.1:9900"

	cn2, err := NewConsulClusterNode(id2, ":9900", dir2)
	if err != nil {
		panic(err)
	}
	defer cn2.Close()

	cn1.Declare("a")

	// propagation delay
	time.Sleep(1000 * time.Millisecond)

	msg := Msg([]byte("hello"))

	debugf("pushing...\n")
	err = cn2.Push("a", msg)
	if err != nil {
		panic(err)
	}

	debugf("polling...\n")
	got, err := cn1.Poll("a")
	if err != nil {
		panic(err)
	}

	if got == nil || !got.Equal(msg) {
		t.Fatal("didn't get the message")
	}
}
