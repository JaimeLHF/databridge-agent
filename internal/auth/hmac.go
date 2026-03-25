package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// Sign computa HMAC-SHA256 do body usando o secret.
// Retorna o hash em hex string.
func Sign(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
