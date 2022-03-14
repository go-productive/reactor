// +build linux

package reactor

type (
	_Node struct {
		bs         []byte
		next       *_Node
		writeIndex int
	}
	_LinkBuf struct {
		head *_Node
		tail *_Node
	}
)

func (n *_Node) bytes() []byte {
	return n.bs[n.writeIndex:]
}

func (n *_Node) discard(writeN int) (int, bool) {
	leftSize := len(n.bs) - n.writeIndex
	if writeN < leftSize {
		n.writeIndex += writeN
		return 0, true
	}
	return writeN - leftSize, false
}

func (l *_LinkBuf) bytes() (bss [][]byte, total int) {
	for node := l.head; node != nil; node = node.next {
		bs := node.bytes()
		bss = append(bss, bs)
		total += len(bs)
	}
	return bss, total
}

func (l *_LinkBuf) isEmpty() bool {
	return l.head == nil
}

func (l *_LinkBuf) discard(writeN int) {
	for writeN > 0 {
		leftWriteN, stillLeft := l.head.discard(writeN)
		if stillLeft {
			return
		}
		l.head = l.head.next
		if l.head == nil {
			l.tail = nil
		}
		writeN = leftWriteN
	}
}

func (l *_LinkBuf) write(bs []byte) {
	node := &_Node{bs: bs}
	if l.head == nil {
		l.head = node
		l.tail = node
		return
	}
	l.tail.next = node
	l.tail = node
}
