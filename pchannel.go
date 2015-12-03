package pchannel

import (
	"errors"
	"time"
)

const (
	CHANNEL_FULL          = 2
	PRIORITY_OUT_OF_INDEX = 1
)

type PChanError struct {
	Code int
}

func NewPChanError(code int) *PChanError {
	return &PChanError{Code: code}
}

func (self *PChanError) Error() string {
	if self.Code == CHANNEL_FULL {
		return "channel is full"
	} else if self.Code == PRIORITY_OUT_OF_INDEX {
		return "priority out of index"
	}
	return ""
}

type PChan struct {
	chs      []chan interface{}
	sleepMS  time.Duration
	closeAll bool
	capacity int
}

func NewPChan(levels int, capacity int) *PChan {
	ret := PChan{}
	ret.chs = []chan interface{}{}
	ret.sleepMS = 1
	ret.closeAll = false
	ret.capacity = capacity

	for i := 0; i < levels; i++ {
		ret.chs = append(ret.chs, make(chan interface{}, capacity*(levels-i)))
	}
	return &ret
}

func (self *PChan) Stat() map[string]interface{} {
	ret := make(map[string]interface{})
	ret["sleepMS"] = self.sleepMS
	ret["capacity"] = self.capacity
	ret["closeAll"] = self.closeAll
	chanStat := []int{}
	for _, ch := range self.chs {
		chanStat = append(chanStat, len(ch))
	}
	ret["chs"] = chanStat
	return ret
}

func (self *PChan) Close() {
	for _, ch := range self.chs {
		close(ch)
	}
	self.closeAll = true
}

func (self *PChan) Push(priority int, val interface{}) error {
	if priority >= len(self.chs) || priority < 0 {
		return NewPChanError(PRIORITY_OUT_OF_INDEX)
	}

	idx := len(self.chs) - priority - 1
	if len(self.chs[idx]) > self.capacity*(priority+1)/2 {
		return NewPChanError(CHANNEL_FULL)
	}

	self.chs[idx] <- val
	return nil
}

func (self *PChan) Size() int {
	count := 0
	for _, ch := range self.chs {
		count += len(ch)
	}
	return count
}

func (self *PChan) Pop() (interface{}, error) {
	for _, ch := range self.chs {
		if len(ch) > 0 {
			self.sleepMS = 1
			return <-ch, nil
		}
	}
	if self.closeAll {
		return nil, errors.New("channel is closed")
	}
	time.Sleep(self.sleepMS * time.Millisecond)
	self.sleepMS *= 2
	if self.sleepMS > 1000 {
		self.sleepMS = 1000
	}
	return nil, nil
}

func (self *PChan) QuickPop() (interface{}, error) {
	for _, ch := range self.chs {
		if len(ch) > 0 {
			self.sleepMS = 1
			return <-ch, nil
		}
	}
	if self.closeAll {
		return nil, errors.New("channel is closed")
	}
	return nil, nil
}
