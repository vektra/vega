package mailbox

import "testing"

func TestRouterAdd(t *testing.T) {
	r := registry()

	d := testRouter()
	d.Add("a", r)

	r2, ok := d.DiscoverEndpoint("a")

	if !ok {
		t.Fatal("router didn't get told about a registry")
	}

	if r2 != r {
		t.Fatal("didn't return the right registry")
	}
}

func TestRouterPush(t *testing.T) {
	r := registry()
	r.Declare("a")

	d := testRouter()
	d.Add("a", r)

	msg := Msg([]byte("hello"))

	err := d.Push("a", msg)
	if err != nil {
		panic(err)
	}

	res, _ := r.Poll("a")

	if !res.Equal(msg) {
		t.Fatal("router didn't route")
	}
}
