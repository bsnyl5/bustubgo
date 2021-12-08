package buff

import (
	"crypto/rand"
	"log"
	"net/http"
	_ "net/http/pprof"
	"testing"

	"github.com/stretchr/testify/assert"
)

func init() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func Test_BPMBinaryDataTest(t *testing.T) {
	disk := NewDiskManager("test.db")
	poolSize := 10
	bpm := NewBufferPool(poolSize, disk)

	page := bpm.NewPage()
	assert.NotNil(t, page)
	assert.Equal(t, 0, page.pageID)
	var randomBinData [PageSize]byte
	_, err := rand.Read(randomBinData[:])
	assert.NoError(t, err)

	randomBinData[PageSize/2] = '0'
	randomBinData[PageSize-1] = '0'
	copy(page.GetData(), randomBinData[:])

	// still can create poolSize - 1 more page
	for i := 1; i < poolSize; i++ {
		assert.NotNil(t, bpm.NewPage())
	}

	// can't create more page
	for i := poolSize; i < poolSize*2; i++ {
		assert.Nil(t, bpm.NewPage())
	}

	// should be able to create more 5 page after unpinning 5 page
	for i := 0; i < 5; i++ {
		assert.True(t, bpm.UnpinPage(i, true))
		bpm.FlushPage(i)
	}

	for i := 0; i < 5; i++ {
		p := bpm.NewPage()
		assert.NotNil(t, p)
		bpm.UnpinPage(p.pageID, false)
	}

	page0, err := bpm.FetchPage(0)
	assert.NoError(t, err)
	assert.Equal(t, page0.GetData(), randomBinData[:])
	assert.True(t, bpm.UnpinPage(0, true))

}

func Test_BPMSampleTest(t *testing.T) {
	disk := NewDiskManager("test.db")
	poolSize := 10
	bpm := NewBufferPool(poolSize, disk)

	page := bpm.NewPage()
	assert.NotNil(t, page)
	page.Write([]byte("Hello"))
	assert.Equal(t, []byte("Hello"), page.GetData()[:5])
	for i := 1; i < poolSize; i++ {
		assert.NotNil(t, bpm.NewPage())
	}

	for i := poolSize; i < poolSize*2; i++ {
		assert.Nil(t, bpm.NewPage())
	}

	for i := 0; i < 5; i++ {
		assert.True(t, bpm.UnpinPage(i, true))
	}

	for i := 0; i < 4; i++ {
		assert.NotNil(t, bpm.NewPage())
	}
	page0, _ := bpm.FetchPage(0)
	assert.Equal(t, []byte("Hello"), page0.GetData()[:5])
	assert.True(t, bpm.UnpinPage(0, true))

	newPage := bpm.NewPage()
	assert.NotNil(t, newPage)

	page0, err := bpm.FetchPage(0)
	assert.Nil(t, page0)
	assert.Error(t, err)
}
