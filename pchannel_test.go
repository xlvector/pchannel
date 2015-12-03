package pchannel

import (
	"testing"
)

type A struct {
	b string
	c int
}

func TestPChannel(t *testing.T) {
	pch := NewPChan(3, 10)

	pch.Push(0, A{b : "aaa", c: 0})
	pch.Push(2, A{b : "bbb", c: 2})
	pch.Push(1, A{b : "ccc", c: 1})
	pch.Push(1, A{b : "ddd", c: 1})
	pch.Push(2, A{b : "eee", c: 2})
	pch.Push(0, A{b : "fff", c: 0})
	pch.Push(0, A{b : "ggg", c: 0})
	pch.Close()

	n := 0
	check := []string{"bbb", "eee", "ccc", "ddd", "aaa", "fff", "ggg"}
	for{
		val, err := pch.Pop()
		if err != nil {
			break
		}
		if val == nil {
			continue
		}
		vala, ok := val.(A)
		if !ok {
			t.Error()
		}
		if vala.b != check[n] {
			t.Error()
		}
		n += 1
	}
	if n != len(check) {
		t.Error()
	}
}