package main

import (
	"container/list"
	"fmt"
)

func main() {
	l := list.New()
	l.PushFront(1)
	elem := l.PushFront(2)

	fmt.Println(l.Front().Value)
	l.Remove(elem)
	fmt.Println(l.Front().Value)

}
