package omq

type Message struct {
	parts [][]byte
}

func NewMessage(parts ...[]byte) Message {
	out := Message{parts: make([][]byte, len(parts))}
	for i, part := range parts {
		out.parts[i] = append([]byte(nil), part...)
	}
	return out
}

func Bytes(data []byte) Message {
	return NewMessage(data)
}

func String(data string) Message {
	return NewMessage([]byte(data))
}

func Multipart(parts ...[]byte) Message {
	return NewMessage(parts...)
}

func (m Message) Parts() [][]byte {
	out := make([][]byte, len(m.parts))
	for i, part := range m.parts {
		out[i] = append([]byte(nil), part...)
	}
	return out
}

func (m Message) partsView() [][]byte {
	return m.parts
}

func (m Message) Bytes() []byte {
	if len(m.parts) == 0 {
		return nil
	}
	return m.parts[0]
}

func (m Message) String() string {
	return string(m.Bytes())
}

func (m Message) Len() int {
	return len(m.parts)
}

func (m Message) Empty() bool {
	return len(m.parts) == 0
}
