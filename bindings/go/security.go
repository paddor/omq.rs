package omq

func GenerateCurveKeypair() (CurveKeypair, error) {
	return curveKeypairNative()
}

func CurvePublic(secretKey string) (string, error) {
	return curvePublicNative(secretKey)
}
