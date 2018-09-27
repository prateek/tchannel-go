// Copyright (c) 2015 Uber Technologies, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package thrift

import (
	"errors"
	"io"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
)

var (
	errNoBytesRead = errors.New("no bytes read")
)

// OptimizedTRichTransport implements thrift.TRichTransport interface to avoid
// allocating a byte for each operation.
type OptimizedTRichTransport struct {
	io.Writer
	io.Reader
	readBuf  [1]byte
	writeBuf [1]byte
}

var _ thrift.TRichTransport = &OptimizedTRichTransport{}

func (t *OptimizedTRichTransport) Open() error  { return nil }
func (t *OptimizedTRichTransport) Flush() error { return nil }
func (t *OptimizedTRichTransport) IsOpen() bool { return true }
func (t *OptimizedTRichTransport) Close() error { return nil }

func (t *OptimizedTRichTransport) ReadByte() (byte, error) {
	v := t.readBuf[0:1]

	var n int
	var err error

	for {
		n, err = t.Read(v)
		if n > 0 || err != nil {
			break
		}
	}

	if err == io.EOF && n > 0 {
		err = nil
	}
	return v[0], err
}

func (t *OptimizedTRichTransport) WriteByte(b byte) error {
	v := t.writeBuf[:1]

	v[0] = b
	_, err := t.Write(v)
	return err
}

func (t *OptimizedTRichTransport) WriteString(s string) (int, error) {
	return io.WriteString(t.Writer, s)
}

// RemainingBytes returns the max number of bytes (same as Thrift's StreamTransport) as we
// do not know how many bytes we have left.
func (t *OptimizedTRichTransport) RemainingBytes() uint64 {
	const maxSize = ^uint64(0)
	return maxSize
}

// PooledProtocol is a protocol grouped with a transport to enable pooling.
type PooledProtocol struct {
	Transport *OptimizedTRichTransport
	Protocol  thrift.TProtocol
}

// ProtocolPool is a pool for managing and re-using PooledProtocol(s).
type ProtocolPool interface {
	// Get retrieves a protocol from the pool.
	Get() *PooledProtocol

	// Release releases a protocol back to the pool.
	Release(p *PooledProtocol)
}

// DefaultProtocolPool uses the SyncProtocolPool.
var DefaultProtocolPool = NewSyncProtocolPool()

type syncProtocolPool struct {
	pool *sync.Pool
}

func (p syncProtocolPool) Get() *PooledProtocol {
	return p.pool.Get().(*PooledProtocol)
}

func (p syncProtocolPool) Release(value *PooledProtocol) {
	p.pool.Put(value)
}

func NewSyncProtocolPool() ProtocolPool {
	return &syncProtocolPool{
		pool: &sync.Pool{
			New: func() interface{} {
				transport := &OptimizedTRichTransport{}
				protocol := thrift.NewTBinaryProtocolTransport(transport)
				return &PooledProtocol{transport, protocol}
			},
		},
	}
}

func getProtocolWriter(pool ProtocolPool, writer io.Writer) *PooledProtocol {
	wp := pool.Get()
	wp.Transport.Reader = nil
	wp.Transport.Writer = writer
	return wp
}

func getProtocolReader(pool ProtocolPool, reader io.Reader) *PooledProtocol {
	wp := pool.Get()
	wp.Transport.Reader = reader
	wp.Transport.Writer = nil
	return wp
}
