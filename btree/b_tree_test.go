package btree

import (
	"buff"
	"bufio"
	"os"
	"testing"
)

func Test_PrintBtree(t *testing.T) {
	r := bufio.NewReader(bufio.NewReader(os.Stdin))

	var (
		leafMaxSize, internalMaxSize int
	)
	dm := buff.NewDiskManager("test.db")
	bpm := buff.NewBufferPool(100, dm)

	headerPage := bpm.NewPage()
	leafMaxSize = readInt(r)
	internalMaxSize = readInt(r)
	tree := NewTree("foo_pk", bpm, IntComp, leafMaxSize, internalMaxSize)
	tx := NewTx(0)

inloop:
	for {
		instruction, arg := readInstruction(r)
		switch instruction {
		case "c":
			tree.RemoveFromFile(arg, tx)
		case "d":
			tree.Remove(parseInt(arg), tx)
		case "i":
			key := parseInt(arg)
			tree.Insert(key, rid{
				key, key,
			}, tx)
		case "f":
			tree.InsertFromFile(arg, tx)
		case "q":
			break inloop
		case "p":
			tree.Print(bpm)
		}
	}
	bpm.UnpinPage(headerPage.GetPageID(), true)
	// create and fetch header_page
}
