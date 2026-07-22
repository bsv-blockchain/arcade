package api_server

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	arcerrors "github.com/bsv-blockchain/arcade/errors"
)

// TestRespondSubmitError_ARCFieldsAdditive pins the additive 400 body
// contract (external feedback item 2): the legacy "error"/"reason" fields are
// byte-compatible with what existing clients parse, and an ArcError in the
// chain adds the machine-readable taxonomy alongside — numeric "status",
// "title", "detail", "type" — without displacing anything. A plain error adds
// no ARC fields.
func TestRespondSubmitError_ARCFieldsAdditive(t *testing.T) {
	gin.SetMode(gin.TestMode)

	do := func(err error) map[string]any {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		respondSubmitError(c, "ab12", err)
		var body map[string]any
		if uErr := json.Unmarshal(w.Body.Bytes(), &body); uErr != nil {
			t.Fatalf("decode body: %v (%s)", uErr, w.Body.String())
		}
		if w.Code != 400 {
			t.Fatalf("expected 400, got %d", w.Code)
		}
		return body
	}

	arcErr := arcerrors.NewArcError(errors.New("transaction is not final"), arcerrors.StatusNotFinal)
	body := do(arcErr)
	if body["error"] != "transaction failed validation" || body["txid"] != "ab12" {
		t.Errorf("legacy fields wrong: %v", body)
	}
	if body["reason"] != arcErr.Error() {
		t.Errorf("reason = %v, want %q", body["reason"], arcErr.Error())
	}
	if body["status"] != float64(476) {
		t.Errorf("status = %v, want 476", body["status"])
	}
	if body["title"] != "Transaction not final" {
		t.Errorf("title = %v", body["title"])
	}
	if body["detail"] == "" || body["type"] == "" {
		t.Errorf("detail/type missing: %v", body)
	}

	plain := do(errors.New("failed to parse"))
	if plain["reason"] != "failed to parse" {
		t.Errorf("plain reason = %v", plain["reason"])
	}
	for _, k := range []string{"status", "title", "detail", "type"} {
		if _, present := plain[k]; present {
			t.Errorf("plain error must not add ARC field %q: %v", k, plain)
		}
	}
}

// TestArcStatusCodeOf pins the code extraction used to persist StatusCode on
// intake-rejected rows.
func TestArcStatusCodeOf(t *testing.T) {
	if got := arcStatusCodeOf(arcerrors.NewArcError(errors.New("x"), arcerrors.StatusUnlockingScripts)); got != 461 {
		t.Errorf("got %d, want 461", got)
	}
	if got := arcStatusCodeOf(errors.New("plain")); got != 0 {
		t.Errorf("got %d, want 0 for a plain error", got)
	}
}
