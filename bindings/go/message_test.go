package omq

import "testing"

func TestMessageAccessorsAreBoundsSafe(t *testing.T) {
	msg := Multipart([]byte("route"), []byte("body"))

	if part, ok := msg.PartOK(-1); ok || part != nil {
		t.Fatalf("PartOK(-1) = %q/%v, want nil/false", part, ok)
	}
	if part, ok := msg.PartOK(2); ok || part != nil {
		t.Fatalf("PartOK(2) = %q/%v, want nil/false", part, ok)
	}
	if part := msg.Part(2); part != nil {
		t.Fatalf("Part(2) = %q, want nil", part)
	}
	if _, ok := msg.BytesOK(); ok {
		t.Fatal("BytesOK succeeded for multipart message")
	}
}

func TestMessageRouteGroupBodyAndEquality(t *testing.T) {
	body := Multipart([]byte("a"), []byte("b"))
	routed := Route([]byte("worker-1"), body)

	route, ok := routed.RouteOK()
	if !ok || string(route) != "worker-1" {
		t.Fatalf("route = %q/%v, want worker-1/true", route, ok)
	}
	if !routed.Body().Equal(body) {
		t.Fatalf("body = %#v, want %#v", routed.Body().Parts(), body.Parts())
	}

	grouped := Group("weather", []byte("rain"))
	group, ok := grouped.GroupOK()
	if !ok || group != "weather" {
		t.Fatalf("group = %q/%v, want weather/true", group, ok)
	}
	if grouped.Group() != "weather" {
		t.Fatalf("Group = %q, want weather", grouped.Group())
	}
	if got := grouped.Body().String(); got != "rain" {
		t.Fatalf("body = %q, want rain", got)
	}
}

func TestMessageSinglePartBytesOKCopies(t *testing.T) {
	msg := String("copy")
	bytes, ok := msg.BytesOK()
	if !ok || string(bytes) != "copy" {
		t.Fatalf("BytesOK = %q/%v, want copy/true", bytes, ok)
	}
	bytes[0] = 'x'
	if got := msg.String(); got != "copy" {
		t.Fatalf("message = %q, want copy", got)
	}
}
