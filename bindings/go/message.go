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

func (m Message) BytesOK() ([]byte, bool) {
	if len(m.parts) != 1 {
		return nil, false
	}
	return append([]byte(nil), m.parts[0]...), true
}

func (m Message) Part(index int) []byte {
	part, ok := m.PartOK(index)
	if !ok {
		return nil
	}
	return part
}

func (m Message) PartOK(index int) ([]byte, bool) {
	if index < 0 || index >= len(m.parts) {
		return nil, false
	}
	return append([]byte(nil), m.parts[index]...), true
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

func (m Message) Route() []byte {
	return m.Part(0)
}

func (m Message) RouteOK() ([]byte, bool) {
	return m.PartOK(0)
}

func (m Message) Group() string {
	group, ok := m.GroupOK()
	if !ok {
		return ""
	}
	return group
}

func (m Message) GroupOK() (string, bool) {
	part, ok := m.PartOK(0)
	if !ok {
		return "", false
	}
	return string(part), true
}

func (m Message) Body() Message {
	if len(m.parts) <= 1 {
		return Message{}
	}
	return NewMessage(m.parts[1:]...)
}

func (m Message) Equal(other Message) bool {
	if len(m.parts) != len(other.parts) {
		return false
	}
	for i, part := range m.parts {
		if string(part) != string(other.parts[i]) {
			return false
		}
	}
	return true
}
