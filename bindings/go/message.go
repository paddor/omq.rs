package omq

// Message is an immutable OMQ message with zero or more parts.
type Message struct {
	parts     [][]byte
	routingID uint32
}

// NewMessage copies parts into a message.
func NewMessage(parts ...[]byte) Message {
	out := Message{parts: make([][]byte, len(parts))}
	for i, part := range parts {
		out.parts[i] = append([]byte(nil), part...)
	}
	return out
}

// Bytes creates a single-part binary message.
func Bytes(data []byte) Message {
	return NewMessage(data)
}

// String creates a single-part UTF-8 message.
func String(data string) Message {
	return NewMessage([]byte(data))
}

// Multipart creates a multipart message.
func Multipart(parts ...[]byte) Message {
	return NewMessage(parts...)
}

// Group creates a RADIO/DISH group message.
func Group(group string, body []byte) Message {
	return Multipart([]byte(group), body)
}

// WithRoutingID attaches the opaque routing ID supplied by a SERVER socket.
func (m Message) WithRoutingID(routingID uint32) Message {
	m.routingID = routingID
	return m
}

// RoutingID returns the SERVER routing ID and whether it is present.
func (m Message) RoutingID() (uint32, bool) {
	return m.routingID, m.routingID != 0
}

// Route prefixes a message with a routing identity.
func Route(identity []byte, body Message) Message {
	parts := make([][]byte, 0, len(body.parts)+1)
	parts = append(parts, identity)
	parts = append(parts, body.parts...)
	return NewMessage(parts...)
}

// Parts returns copies of all message parts.
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

// Bytes returns a copy of the first part.
func (m Message) Bytes() []byte {
	part := m.Part(0)
	return part
}

// BytesOK returns the single part when the message has exactly one part.
func (m Message) BytesOK() ([]byte, bool) {
	if len(m.parts) != 1 {
		return nil, false
	}
	return append([]byte(nil), m.parts[0]...), true
}

// Part returns a copy of one part or nil when index is out of range.
func (m Message) Part(index int) []byte {
	part, ok := m.PartOK(index)
	if !ok {
		return nil
	}
	return part
}

// PartOK returns a copy of one part and whether it exists.
func (m Message) PartOK(index int) ([]byte, bool) {
	if index < 0 || index >= len(m.parts) {
		return nil, false
	}
	return append([]byte(nil), m.parts[index]...), true
}

// String decodes the first part as UTF-8 text.
func (m Message) String() string {
	if len(m.parts) == 0 {
		return ""
	}
	return string(m.parts[0])
}

// Len returns the number of parts.
func (m Message) Len() int {
	return len(m.parts)
}

// IsMultipart reports whether the message has more than one part.
func (m Message) IsMultipart() bool {
	return len(m.parts) > 1
}

// ByteLen returns the total bytes across all parts.
func (m Message) ByteLen() int {
	var total int
	for _, part := range m.parts {
		total += len(part)
	}
	return total
}

// Empty reports whether the message has no parts.
func (m Message) Empty() bool {
	return len(m.parts) == 0
}

// Route returns the first part as a routing identity.
func (m Message) Route() []byte {
	return m.Part(0)
}

// RouteOK returns the routing identity and whether it exists.
func (m Message) RouteOK() ([]byte, bool) {
	return m.PartOK(0)
}

// Group returns the first part as a group name.
func (m Message) Group() string {
	group, ok := m.GroupOK()
	if !ok {
		return ""
	}
	return group
}

// GroupOK returns the group name and whether it exists.
func (m Message) GroupOK() (string, bool) {
	part, ok := m.PartOK(0)
	if !ok {
		return "", false
	}
	return string(part), true
}

// Body returns all parts after the first part.
func (m Message) Body() Message {
	if len(m.parts) <= 1 {
		return Message{}
	}
	return NewMessage(m.parts[1:]...)
}

// Equal reports whether two messages have identical parts.
func (m Message) Equal(other Message) bool {
	if m.routingID != other.routingID || len(m.parts) != len(other.parts) {
		return false
	}
	for i, part := range m.parts {
		if string(part) != string(other.parts[i]) {
			return false
		}
	}
	return true
}
