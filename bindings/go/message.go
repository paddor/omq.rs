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

func Group(group string, body []byte) Message {
	return Multipart([]byte(group), body)
}

func Route(identity []byte, body Message) Message {
	parts := make([][]byte, 0, len(body.parts)+1)
	parts = append(parts, identity)
	parts = append(parts, body.parts...)
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
	part := m.Part(0)
	return part
}

func (m Message) Part(index int) []byte {
	if len(m.parts) == 0 {
		return nil
	}
	return append([]byte(nil), m.parts[index]...)
}

func (m Message) String() string {
	if len(m.parts) == 0 {
		return ""
	}
	return string(m.parts[0])
}

func (m Message) Len() int {
	return len(m.parts)
}

func (m Message) IsMultipart() bool {
	return len(m.parts) > 1
}

func (m Message) ByteLen() int {
	var total int
	for _, part := range m.parts {
		total += len(part)
	}
	return total
}

func (m Message) Empty() bool {
	return len(m.parts) == 0
}
