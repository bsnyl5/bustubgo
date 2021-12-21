package btree

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

func IntComp(lhs interface{}, rhs interface{}) bool {
	return lhs.(int) > rhs.(int)
}
func readInt(r *bufio.Reader) int {
	input, err := r.ReadString('\n')
	if err != nil {
		panic(err)
	}
	return parseInt(input)
}

func readInstruction(r *bufio.Reader) (instruction, arg string) {
	input, err := r.ReadString('\n')
	if err != nil {
		panic(err)
	}
	parts := strings.Split(input, " ")
	switch len(parts) {
	case 1:
		return parts[0], ""
	case 2:
		return parts[0], parts[1]
	default:
		panic(fmt.Errorf("invalid line %s", input))
	}
}
func parseInt(str string) int {
	ret, err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return ret
}
