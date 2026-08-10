package omq

// GenerateCurveKeypair generates a CURVE Z85 key pair.
func GenerateCurveKeypair() (CurveKeypair, error) {
	return curveKeypairNative()
}

// CurvePublic derives a CURVE public key from a Z85 secret key.
func CurvePublic(secretKey string) (string, error) {
	return curvePublicNative(secretKey)
}
