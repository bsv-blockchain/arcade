package propagation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	btchainhash "github.com/bsv-blockchain/go-bt/v2/chainhash"
	tnerr "github.com/bsv-blockchain/teranode/errors"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"

	arcerrors "github.com/bsv-blockchain/arcade/errors"
	"github.com/bsv-blockchain/arcade/models"
)

// The arcade ↔ teranode rejection-reason contract.
//
// A transaction arcade marks REJECTED must always carry a reason its submitter
// can act on, and a transaction teranode did NOT reject must never be
// terminalized. Both properties depend on a boundary arcade does not own:
// teranode collapses its wrapped error chain to a single "CODE (n): message"
// pair (errors.UserMessage → DeepestPublicCause), keeping only the innermost
// cause whose code is on publicCauseCodes and discarding everything else —
// including the "[ProcessTransaction][<txid>]" wrapper that is arcade's only
// reliable way to bind a verdict to a transaction.
//
// Every fixture below is therefore GENERATED, not hand-written: each case
// constructs the same error chain the corresponding teranode call site builds
// and renders it through teranode's own errors.UserMessage. A hand-written
// line can encode a shape teranode never emits (and the pre-existing fixtures
// in this package do exactly that: they all carry a [ProcessTransaction]
// wrapper that the deepest-cause rule strips whenever an allowlisted code is
// present). Generating them means a teranode module bump surfaces boundary
// drift here as a test failure instead of in production.
//
// Each row runs twice: chunk cap 1 ("single mode" — Propagation.
// TeranodeMaxBatchSize=1, one tx per POST /txs) and chunk cap 1024 ("batch
// mode", the target tx sharing one POST with three healthy siblings). Both
// must reach the same verdict, and batch mode must additionally leave the
// siblings' acceptances intact.
//
// See docs/teranode-error-surfacing.md for the matrix these rows encode.

// --- fixture construction ------------------------------------------------

// wrapProcessTransaction mirrors teranode's outermost propagation wrapper,
// services/propagation/Server.go: NewProcessingError("[ProcessTransaction][%s]
// failed to validate transaction", txID, err).
func wrapProcessTransaction(txid string, inner error) error {
	return tnerr.NewProcessingError("[ProcessTransaction][%s] failed to validate transaction", txid, inner)
}

// wrapValidate mirrors Validator.Validate's wrapper around the TxValidator/BDK
// result, services/validator/Validator.go: NewProcessingError("[Validate][%s]
// error validating transaction", txID, err).
func wrapValidate(txid string, inner error) error {
	return tnerr.NewProcessingError("[Validate][%s] error validating transaction", txid, inner)
}

// wrapSpend mirrors the wrapper Validator.Validate puts around a failed
// SpendAndCreate: NewProcessingError("[Validate][%s] error spending utxos",
// txID, err).
func wrapSpend(txid string, inner error) error {
	return tnerr.NewProcessingError("[Validate][%s] error spending utxos", txid, inner)
}

// boundaryLine renders one failed transaction the way teranode's /txs and /tx
// handlers do — errors.UserMessage, the shared public error boundary.
func boundaryLine(chain error) string {
	return tnerr.UserMessage(chain)
}

// bdkScriptFailure stands in for the opaque GoBDK cause: a plain error, not a
// *tnerr.Error, exactly like the bdkscript errors the adapter wraps. Being a
// plain error is the whole problem — DeepestPublicCause can never select it,
// so its text cannot cross the boundary.
//
// The C rows are the one place these fixtures MIRROR teranode rather than call
// it: mapBDKValidationError is unexported and needs a real bdkscript error, so
// the chain it builds is reproduced here. When teranode starts folding the
// cause into the message (services/validator/ScriptVerifierGoBDK.go), update
// these rows to match and drop their upstreamGap. Every other row is generated
// by teranode's own constructors and needs no maintenance.
func bdkScriptFailure(code string) error {
	return errors.New(code + ": script evaluation failed")
}

// utxoSpentError builds the conflict-family error the UTXO stores produce, via
// teranode's own constructor, so the wrapper-less "<txid>:<vout> utxo already
// spent by tx <spender>[n]" shape (and its doubled code prefix) is genuine.
func utxoSpentError(t *testing.T, outpointTxid string, vout uint32, spenderTxid string) error {
	t.Helper()
	owner, err := btchainhash.NewHashFromStr(outpointTxid)
	if err != nil {
		t.Fatalf("NewHashFromStr(%s): %v", outpointTxid, err)
	}
	spender, err := btchainhash.NewHashFromStr(spenderTxid)
	if err != nil {
		t.Fatalf("NewHashFromStr(%s): %v", spenderTxid, err)
	}
	return tnerr.NewUtxoSpentError(*owner, vout, *owner, &spendpkg.SpendingData{TxID: spender, Vin: 0})
}

// --- the contract table --------------------------------------------------

// boundaryOutcome is what arcade must do with the transaction.
type boundaryOutcome int

const (
	// outcomeRejected: a verdict about these bytes — terminalize REJECTED
	// with a reason the submitter can act on.
	outcomeRejected boundaryOutcome = iota
	// outcomeRetry: not a verdict (a condition of the node's view, or a
	// node-side infra fault) — never terminalize; requeue.
	outcomeRetry
)

func (o boundaryOutcome) String() string {
	if o == outcomeRejected {
		return "REJECTED"
	}
	return "retry"
}

// boundaryCase is one row of the matrix.
type boundaryCase struct {
	// row is the matrix identifier from docs/teranode-error-surfacing.md.
	row string
	// what names the teranode failure this row reproduces.
	what string
	// build constructs the error chain teranode's call site builds. target is
	// the submitted txid; other is a second txid the message may reference
	// (missing parent, competing spender).
	build func(t *testing.T, target, other string) error
	// status is the HTTP status teranode's httpStatusForTxError assigns.
	status int
	// want is the outcome arcade must produce.
	want boundaryOutcome
	// wantReason is a substring the persisted ExtraInfo must contain. Only
	// meaningful when want == outcomeRejected.
	wantReason string
	// wantARC, when non-zero, is the ARC status code the row must carry.
	wantARC int
	// upstreamGap, when set, records that the surfaced reason is known to be
	// uninformative and can only be fixed in teranode. mustNotContain must then
	// stay absent from the reason; when teranode starts carrying the real
	// cause, this row fails and gets promoted to a normal row.
	upstreamGap    string
	mustNotContain string
}

// spentOutpointVout is the vout every fixture transaction spends. Kept in one
// place because the conflict-family rows must name the outpoint the submitted
// transaction actually spends for outpoint attribution to resolve.
const spentOutpointVout = 0

func boundaryCases() []boundaryCase {
	return []boundaryCase{
		// --- A. propagation server, before the validator -----------------
		{
			row:    "A1",
			what:   "transaction larger than MaxTxSizePolicy",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, _, _ string) error {
				// Returned directly by processTransaction — no wrapper, and
				// the message names no txid.
				return tnerr.NewTxInvalidError("[ProcessTransaction] transaction size %d exceeds maximum allowed size %d", 12_000_000, 10_485_760)
			},
			want:       outcomeRejected,
			wantReason: "exceeds maximum allowed size",
		},
		{
			row:    "A4",
			what:   "coinbase submitted",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, target, _ string) error {
				return tnerr.NewTxInvalidError("[ProcessTransaction][%s] received coinbase transaction", target)
			},
			want:       outcomeRejected,
			wantReason: "received coinbase transaction",
		},
		{
			row:    "A5",
			what:   "transaction with no outputs",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, target, _ string) error {
				return tnerr.NewTxInvalidError("[ProcessTransaction][%s] received transaction with no outputs", target)
			},
			want:       outcomeRejected,
			wantReason: "no outputs",
		},
		{
			row:    "A6",
			what:   "duplicate input",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, target, other string) error {
				return tnerr.NewTxInvalidError("[ProcessTransaction][%s] duplicate input found: %s:%d", target, other, 0)
			},
			want:       outcomeRejected,
			wantReason: "duplicate input found",
		},
		{
			row:    "A7",
			what:   "node blob store write failed",
			status: http.StatusInternalServerError,
			build: func(_ *testing.T, target, _ string) error {
				return tnerr.NewStorageError("[ProcessTransaction][%s] failed to save transaction", target,
					tnerr.NewStorageError("blob store unavailable"))
			},
			// A node-side infra fault says nothing about these bytes.
			want: outcomeRetry,
		},

		// --- B. Go-side consensus / policy checks ------------------------
		{
			row:    "B3",
			what:   "outputs exceed inputs (bad-txns-in-belowout)",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, target, _ string) error {
				return wrapProcessTransaction(target, wrapValidate(target,
					tnerr.NewTxInvalidError("bad-txns-in-belowout")))
			},
			want:       outcomeRejected,
			wantReason: "bad-txns-in-belowout",
		},
		{
			row:    "B4",
			what:   "BIP68 sequence lock not satisfied",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, target, _ string) error {
				return wrapProcessTransaction(target, wrapValidate(target,
					tnerr.NewTxInvalidError("transaction sequence lock height not satisfied: required %d, current %d", 870_500, 870_123)))
			},
			want:       outcomeRejected,
			wantReason: "sequence lock height not satisfied",
		},
		{
			row:    "B6",
			what:   "input script too large (bad-txns-inputs-too-large)",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, target, _ string) error {
				return wrapProcessTransaction(target, wrapValidate(target,
					tnerr.NewTxPolicyError("bad-txns-inputs-too-large")))
			},
			want:       outcomeRejected,
			wantReason: "bad-txns-inputs-too-large",
		},

		// --- C. BDK script / policy engine -------------------------------
		{
			row:    "C1",
			what:   "script evaluation failure (bad signature)",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, target, _ string) error {
				return wrapProcessTransaction(target, wrapValidate(target,
					tnerr.NewTxInvalidError("GoBDK fail to ValidateTransaction", bdkScriptFailure("SCRIPT_ERR_EVAL_FALSE"))))
			},
			want:       outcomeRejected,
			wantReason: "GoBDK fail to ValidateTransaction",
			upstreamGap: "the bdkscript error is a plain error, so DeepestPublicCause cannot select it; " +
				"teranode must carry it into the TX_INVALID message (ScriptVerifierGoBDK.mapBDKValidationError)",
			mustNotContain: "SCRIPT_ERR",
		},
		{
			row:    "C2",
			what:   "BDK policy rejection (non-standard / sigops)",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, target, _ string) error {
				policyErr := tnerr.NewTxPolicyError("GoBDK fail to ValidateTransaction by policy settings", bdkScriptFailure("DOS_ERR_NOT_STANDARD"))
				return wrapProcessTransaction(target, wrapValidate(target,
					tnerr.NewTxInvalidError("GoBDK fail to ValidateTransaction", policyErr)))
			},
			want:           outcomeRejected,
			wantReason:     "GoBDK fail to ValidateTransaction by policy settings",
			upstreamGap:    "same as C1: the DoS error code never crosses the boundary",
			mustNotContain: "DOS_ERR",
		},
		{
			row:    "C3",
			what:   "fee below the node's floor",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, target, _ string) error {
				policyErr := tnerr.NewTxPolicyError("transaction fee is too low", bdkScriptFailure("DOS_ERR_INSUFFICIENT_FEE"))
				return wrapProcessTransaction(target, wrapValidate(target,
					tnerr.NewTxInvalidError("GoBDK fail to ValidateTransaction", policyErr)))
			},
			want:       outcomeRejected,
			wantReason: "transaction fee is too low",
		},
		{
			row:    "C5",
			what:   "BDK CGO exception",
			status: http.StatusInternalServerError,
			build: func(_ *testing.T, target, _ string) error {
				return wrapProcessTransaction(target, wrapValidate(target,
					tnerr.NewProcessingError("GoBDK fail to ValidateTransaction", bdkScriptFailure("SCRIPT_ERR_CGO_EXCEPTION"))))
			},
			// PROCESSING is not on the allowlist, so nothing about the cause
			// survives; the node classifies it 500, which is arcade's only
			// signal that this is a node fault rather than a verdict.
			want: outcomeRetry,
		},

		// --- D. chain / UTXO state ---------------------------------------
		{
			row:    "D1",
			what:   "non-final against the node's tip",
			status: http.StatusBadRequest,
			build: func(_ *testing.T, target, _ string) error {
				lockErr := tnerr.NewTxLockTimeError("lock time (1783806110) as timestamp is not less than median block time (1783805456)")
				return wrapProcessTransaction(target,
					tnerr.NewUtxoNonFinalError("[Validate][%s] transaction is not final", target, lockErr))
			},
			want:       outcomeRejected,
			wantReason: "not less than median block time",
			wantARC:    int(arcerrors.StatusNotFinal),
		},
		{
			row:    "D2",
			what:   "missing parent (out-of-order child)",
			status: http.StatusUnprocessableEntity,
			build: func(_ *testing.T, target, other string) error {
				return wrapProcessTransaction(target,
					tnerr.NewProcessingError("[Validate][%s] error getting transaction input block heights", target,
						tnerr.NewTxMissingParentError("[Validate][%s] error getting parent transaction %s", target, other, tnerr.ErrTxNotFound)))
			},
			// A condition of the node's view, not a verdict (#254/#295).
			want: outcomeRetry,
		},
		{
			row:    "D3",
			what:   "double spend (utxo already spent)",
			status: http.StatusConflict,
			build: func(t *testing.T, _, other string) error {
				// other is the outpoint owner the submitted tx spends; the
				// competing spender is a third transaction.
				return wrapProcessTransaction(other, wrapSpend(other,
					utxoSpentError(t, other, spentOutpointVout, strings.Repeat("e", 64))))
			},
			want:       outcomeRejected,
			wantReason: "utxo already spent",
			wantARC:    int(arcerrors.StatusConflict),
		},
		{
			row:    "D4",
			what:   "conflicting transaction",
			status: http.StatusConflict,
			build: func(_ *testing.T, target, _ string) error {
				return wrapProcessTransaction(target,
					tnerr.NewTxConflictingError("[Validate][%s] tx is conflicting", target))
			},
			want:       outcomeRejected,
			wantReason: "tx is conflicting",
			wantARC:    int(arcerrors.StatusConflict),
		},
		{
			row:    "D6",
			what:   "spending a locked utxo (Aerospike LUA wording)",
			status: http.StatusConflict,
			build: func(_ *testing.T, _, other string) error {
				// The LUA path names the RECORD's txid — the parent whose
				// utxos are being spent — never the submitted transaction,
				// and carries no "<txid>:<vout>" outpoint to attribute by.
				return wrapProcessTransaction(other, wrapSpend(other,
					tnerr.NewTxLockedError("[SPEND_BATCH_LUA][%s] transaction is locked, blockHeight %d: %d - %s", other, 870_123, 4711, "locked")))
			},
			want:       outcomeRejected,
			wantReason: "transaction is locked",
			wantARC:    int(arcerrors.StatusConflict),
		},
		{
			row:    "D7",
			what:   "frozen utxo",
			status: http.StatusForbidden,
			build: func(_ *testing.T, target, other string) error {
				return wrapProcessTransaction(target, wrapSpend(target,
					tnerr.NewUtxoFrozenError("[SPEND_BATCH_LUA][%s] UTXO is frozen, vout %d: %s", other, spentOutpointVout, "frozen by alert system")))
			},
			want: outcomeRejected,
			// The cause is gone, so all arcade can honestly say is what the
			// peer's own status code means. That is still a real, actionable
			// answer — which is the bar — but it is recovered from the HTTP
			// layer, not from teranode's message.
			wantReason: "HTTP 403",
			wantARC:    int(arcerrors.StatusFrozenPolicy),
			upstreamGap: "ERR_UTXO_FROZEN is not on publicCauseCodes, so the reason collapses to the outer " +
				"PROCESSING wrapper and arcade can only recover the class from the 403; teranode must " +
				"allowlist it for the submitter to learn WHICH utxo is frozen and why",
			mustNotContain: "UTXO_FROZEN",
		},
		{
			row:    "D10",
			what:   "block assembly unavailable",
			status: http.StatusInternalServerError,
			build: func(_ *testing.T, target, _ string) error {
				return wrapProcessTransaction(target,
					tnerr.NewProcessingError("[Validate][%s] error sending tx to block assembler", target,
						tnerr.NewServiceError("error calling blockAssembler Store()")))
			},
			want: outcomeRetry,
		},
	}
}

// --- harness -------------------------------------------------------------

// boundaryServer answers POST /txs with teranode's failure-list body, but only
// for requests whose payload actually carries one of the failing transactions.
// A chunk holding none of them gets a 200, which is what makes the same table
// runnable at chunk cap 1 (four separate POSTs) and at chunk cap 1024 (one
// POST carrying everything).
func boundaryServer(t *testing.T, failing map[string][]byte, lineFor map[string]string, status int, hits *eventLog) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read /txs body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		hits.add("broadcast")

		var lines []string
		for txid, raw := range failing {
			if bytes.Contains(body, raw) {
				lines = append(lines, lineFor[txid])
			}
		}
		if len(lines) == 0 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte("Failed to process transactions:\n" + strings.Join(lines, "\n") + "\n"))
	}))
}

// boundaryFixture is the transaction set one row runs against: the target that
// draws the failure, plus healthy siblings that must be unaffected.
type boundaryFixture struct {
	target    string
	targetRaw []byte
	// outpointOwner is the txid whose output the target spends. Conflict-family
	// messages name it rather than the target.
	outpointOwner string
	siblings      []string
	siblingRaw    [][]byte
}

func newBoundaryFixture(t *testing.T, siblingCount int) boundaryFixture {
	t.Helper()
	owner := strings.Repeat("a", 64)
	target, targetRaw := spendingTx(t, owner, spentOutpointVout, 1)

	f := boundaryFixture{target: target, targetRaw: targetRaw, outpointOwner: owner}
	for i := 0; i < siblingCount; i++ {
		// Each sibling spends a distinct, unrelated outpoint so nothing in the
		// batch depends on anything else in it.
		src := strings.Repeat(fmt.Sprintf("%x", i+2), 64)
		txid, raw := spendingTx(t, src, 0, uint32(100+i))
		f.siblings = append(f.siblings, txid)
		f.siblingRaw = append(f.siblingRaw, raw)
	}
	return f
}

// TestTeranodeRejectionReasonContract is the contract: for every teranode
// failure class, arcade must reach the right outcome, bind it to the right
// transaction, and — when the outcome is REJECTED — persist a reason naming
// the cause. Both broadcast modes, same result.
func TestTeranodeRejectionReasonContract(t *testing.T) {
	modes := []struct {
		name     string
		chunkCap int
		siblings int
	}{
		{name: "single", chunkCap: 1, siblings: 3},
		{name: "batch", chunkCap: 1024, siblings: 3},
	}

	for _, tc := range boundaryCases() {
		for _, mode := range modes {
			t.Run(fmt.Sprintf("%s/%s/%s", tc.row, mode.name, strings.ReplaceAll(tc.what, " ", "_")), func(t *testing.T) {
				runBoundaryCase(t, tc, mode.chunkCap, mode.siblings)
			})
		}
	}
}

func runBoundaryCase(t *testing.T, tc boundaryCase, chunkCap, siblingCount int) {
	t.Helper()

	f := newBoundaryFixture(t, siblingCount)
	line := boundaryLine(tc.build(t, f.target, f.outpointOwner))
	if line == "" {
		t.Fatalf("row %s: teranode boundary rendered an empty line", tc.row)
	}
	t.Logf("row %s wire line: %s", tc.row, line)

	hits := &eventLog{}
	srv := boundaryServer(t,
		map[string][]byte{f.target: f.targetRaw},
		map[string]string{f.target: line},
		tc.status, hits)
	defer srv.Close()

	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, 50, 5*time.Millisecond)
	p.teranodeBatchCap = chunkCap

	admit(t, p, f.target, f.targetRaw)
	for i, sib := range f.siblings {
		admit(t, p, sib, f.siblingRaw[i])
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushSync: %v", err)
	}

	got := ms.lastUpdateForTxid(f.target)

	switch tc.want {
	case outcomeRejected:
		if got == nil || got.Status != models.StatusRejected {
			t.Fatalf("row %s: target must terminalize REJECTED with the teranode verdict, got %+v\n  wire line: %s",
				tc.row, got, line)
		}
		if got.ExtraInfo == "" {
			t.Fatalf("row %s: REJECTED with an empty reason — a submitter has nothing to act on", tc.row)
		}
		if !strings.Contains(got.ExtraInfo, tc.wantReason) {
			t.Errorf("row %s: reason = %q, want it to contain %q", tc.row, got.ExtraInfo, tc.wantReason)
		}
		if tc.wantARC != 0 && got.StatusCode != tc.wantARC {
			t.Errorf("row %s: ARC status = %d, want %d", tc.row, got.StatusCode, tc.wantARC)
		}
		if tc.upstreamGap != "" {
			t.Logf("row %s: documented upstream gap — %s", tc.row, tc.upstreamGap)
			if tc.mustNotContain != "" && strings.Contains(got.ExtraInfo, tc.mustNotContain) {
				t.Errorf("row %s: reason now carries %q — teranode has closed this gap; "+
					"promote the row to a normal one and drop upstreamGap/mustNotContain",
					tc.row, tc.mustNotContain)
			}
		}
	case outcomeRetry:
		if got != nil && got.Status == models.StatusRejected {
			t.Fatalf("row %s: must NOT terminalize (%s), but wrote REJECTED %q\n  wire line: %s",
				tc.row, tc.what, got.ExtraInfo, line)
		}
		waitForPending(t, p, 1)
	}

	// Sibling integrity: transactions teranode did not name must keep their
	// acceptance regardless of what happened to the target. This is the
	// property an unattributable failure line silently destroys — one bad line
	// makes arcade discard the whole chunk's verdicts.
	for _, sib := range f.siblings {
		st := ms.lastUpdateForTxid(sib)
		if st == nil || st.Status != models.StatusAcceptedByNetwork {
			t.Errorf("row %s: sibling %s was not named by teranode but did not keep its acceptance, got %+v",
				tc.row, sib[:8], st)
		}
	}
}

// admit pushes one transaction through the dispatcher without flushing, so a
// caller can assemble a multi-transaction batch and flush it as one POST.
func admit(t *testing.T, p *Propagator, txid string, raw []byte) {
	t.Helper()
	if err := p.handleMessage(context.Background(), consumerMsg(realPropMsg(t, txid, raw))); err != nil {
		t.Fatalf("handleMessage(%s): %v", txid[:8], err)
	}
}

// TestBoundaryLine_DropsTheProcessTransactionWrapper pins the mechanism the
// whole contract turns on, so a future teranode bump that changes it is
// diagnosed here rather than in a dozen downstream assertions: whenever the
// chain contains an allowlisted cause, teranode surfaces THAT cause alone and
// the "[ProcessTransaction][<txid>]" wrapper — arcade's only reliable
// attribution key — is discarded.
func TestBoundaryLine_DropsTheProcessTransactionWrapper(t *testing.T) {
	target := strings.Repeat("f", 64)

	allowlisted := boundaryLine(wrapProcessTransaction(target, wrapValidate(target,
		tnerr.NewTxInvalidError("bad-txns-in-belowout"))))
	if strings.Contains(allowlisted, target) {
		t.Errorf("teranode now carries the txid on an allowlisted cause (%q) — "+
			"the upstream attribution fix has landed; relax arcade's narrowing fallback", allowlisted)
	}
	if !strings.Contains(allowlisted, "bad-txns-in-belowout") {
		t.Errorf("allowlisted cause did not survive the boundary: %q", allowlisted)
	}

	collapsed := boundaryLine(wrapProcessTransaction(target,
		tnerr.NewTxMissingParentError("[Validate][%s] error getting parent transaction %s", target, strings.Repeat("a", 64))))
	if !strings.Contains(collapsed, target) {
		t.Errorf("non-allowlisted cause should collapse to the outer wrapper (which carries the txid), got %q", collapsed)
	}
	if strings.Contains(collapsed, "TX_MISSING_PARENT") {
		t.Errorf("TX_MISSING_PARENT is now allowlisted (%q) — arcade's lineIsMissingParentCondition "+
			"can match again; drop the 422-status fallback", collapsed)
	}
}

// TestBoundaryContract_NoTerminalRejectionWithoutReason is the invariant the
// whole review exists to protect, asserted directly against the store: every
// REJECTED row arcade writes during broadcast carries a non-empty,
// non-placeholder reason.
func TestBoundaryContract_NoTerminalRejectionWithoutReason(t *testing.T) {
	for _, tc := range boundaryCases() {
		if tc.want != outcomeRejected {
			continue
		}
		t.Run(tc.row, func(t *testing.T) {
			f := newBoundaryFixture(t, 0)
			line := boundaryLine(tc.build(t, f.target, f.outpointOwner))
			hits := &eventLog{}
			srv := boundaryServer(t,
				map[string][]byte{f.target: f.targetRaw},
				map[string]string{f.target: line},
				tc.status, hits)
			defer srv.Close()

			ms := newMockStore()
			p := poisonPropagator(srv.URL, ms, 50, 5*time.Millisecond)
			admit(t, p, f.target, f.targetRaw)
			if err := flushSync(t, p); err != nil {
				t.Fatalf("flushSync: %v", err)
			}

			for _, u := range rejectedUpdates(ms) {
				if strings.TrimSpace(u.ExtraInfo) == "" {
					t.Fatalf("row %s: REJECTED row for %s has no reason", tc.row, u.TxID)
				}
				if u.ExtraInfo == "PROCESSING (4): [ProcessTransaction]["+f.target+"] failed to validate transaction" && tc.upstreamGap == "" {
					t.Fatalf("row %s: reason is teranode's generic wrapper and nothing else: %q", tc.row, u.ExtraInfo)
				}
			}
		})
	}
}

// TestNarrowChunk_BoundedRoundTripsAndExactAttribution is the cost and safety
// argument for narrowing. One guilty transaction hidden in a chunk of sixteen,
// drawing a failure line that names nobody — the shape arcade cannot resolve
// from the response alone.
//
// It must settle in a bounded number of extra round trips (bisection: at most
// 2·log2(n)+1 requests, not n), condemn exactly the guilty transaction, and
// return every other transaction accepted. The bound is what makes this safe
// to run on the hot failure path at production batch sizes.
func TestNarrowChunk_BoundedRoundTripsAndExactAttribution(t *testing.T) {
	const total = 16

	type fixtureTx struct {
		txid string
		raw  []byte
	}
	var txs []fixtureTx
	for i := 0; i < total; i++ {
		src := strings.Repeat(fmt.Sprintf("%x", i%16), 64)
		txid, raw := spendingTx(t, src, 0, uint32(500+i))
		txs = append(txs, fixtureTx{txid: txid, raw: raw})
	}
	guilty := txs[11] // deliberately not at a split boundary

	// A txid-less verdict: exactly what teranode emits for a Go-side
	// consensus failure, where the allowlisted cause shadows the wrapper.
	line := boundaryLine(wrapProcessTransaction(guilty.txid, wrapValidate(guilty.txid,
		tnerr.NewTxInvalidError("bad-txns-in-belowout"))))
	if strings.Contains(line, guilty.txid) {
		t.Fatalf("fixture no longer reproduces a txid-less line: %q", line)
	}

	hits := &eventLog{}
	srv := boundaryServer(t,
		map[string][]byte{guilty.txid: guilty.raw},
		map[string]string{guilty.txid: line},
		http.StatusBadRequest, hits)
	defer srv.Close()

	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, 50, 5*time.Millisecond)

	for _, tx := range txs {
		admit(t, p, tx.txid, tx.raw)
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushSync: %v", err)
	}

	got := ms.lastUpdateForTxid(guilty.txid)
	if got == nil || got.Status != models.StatusRejected {
		t.Fatalf("guilty tx: got %+v, want REJECTED", got)
	}
	if !strings.Contains(got.ExtraInfo, "bad-txns-in-belowout") {
		t.Errorf("guilty tx reason = %q, want the real consensus failure", got.ExtraInfo)
	}
	for _, tx := range txs {
		if tx.txid == guilty.txid {
			continue
		}
		st := ms.lastUpdateForTxid(tx.txid)
		if st == nil || st.Status != models.StatusAcceptedByNetwork {
			t.Errorf("innocent tx %s: got %+v, want ACCEPTED_BY_NETWORK", tx.txid[:8], st)
		}
	}

	// 1 initial + 2 per bisection level (one half clean, one half recursing).
	const maxRequests = 2*4 + 1
	if n := hits.count("broadcast"); n > maxRequests {
		t.Errorf("narrowing took %d requests for a chunk of %d; bisection must stay within %d",
			n, total, maxRequests)
	}
}

// TestBroadcast_KeyedFailuresDoNotNarrow pins the other half of the cost
// argument: when every failure line names the transaction it is about — the
// case once teranode carries the txid, and already the case for the wrapper
// shapes it emits today — the chunk settles in a single round trip and the
// narrowing path is never entered.
func TestBroadcast_KeyedFailuresDoNotNarrow(t *testing.T) {
	f := newBoundaryFixture(t, 3)
	line := boundaryLine(tnerr.NewTxInvalidError("[ProcessTransaction][%s] received transaction with no outputs", f.target))

	hits := &eventLog{}
	srv := boundaryServer(t,
		map[string][]byte{f.target: f.targetRaw},
		map[string]string{f.target: line},
		http.StatusBadRequest, hits)
	defer srv.Close()

	ms := newMockStore()
	p := poisonPropagator(srv.URL, ms, 50, 5*time.Millisecond)
	admit(t, p, f.target, f.targetRaw)
	for i, sib := range f.siblings {
		admit(t, p, sib, f.siblingRaw[i])
	}
	if err := flushSync(t, p); err != nil {
		t.Fatalf("flushSync: %v", err)
	}

	if n := hits.count("broadcast"); n != 1 {
		t.Errorf("keyed failure took %d requests; a placeable verdict must settle in one", n)
	}
	if got := ms.lastUpdateForTxid(f.target); got == nil || got.Status != models.StatusRejected {
		t.Fatalf("target: got %+v, want REJECTED", got)
	}
}

// TestBoundaryContract_RetryRowsNeverTerminalizeUnderRepeatedFailure guards the
// other half: a condition or a node fault that persists must not eventually be
// laundered into a REJECTED verdict by the in-memory requeue budget. It parks
// for durable retry instead, and the park reason names what actually happened.
func TestBoundaryContract_RetryRowsNeverTerminalizeUnderRepeatedFailure(t *testing.T) {
	for _, tc := range boundaryCases() {
		if tc.want != outcomeRetry {
			continue
		}
		t.Run(tc.row, func(t *testing.T) {
			f := newBoundaryFixture(t, 0)
			line := boundaryLine(tc.build(t, f.target, f.outpointOwner))
			hits := &eventLog{}
			srv := boundaryServer(t,
				map[string][]byte{f.target: f.targetRaw},
				map[string]string{f.target: line},
				tc.status, hits)
			defer srv.Close()

			ms := newMockStore()
			// A tight budget so the requeue loop exhausts quickly.
			p := poisonPropagator(srv.URL, ms, 2, time.Millisecond)
			admit(t, p, f.target, f.targetRaw)
			if err := flushSync(t, p); err != nil {
				t.Fatalf("flushSync: %v", err)
			}
			// Drive the bounded requeue to exhaustion.
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if st := ms.lastUpdateForTxid(f.target); st != nil && st.Status == models.StatusPendingRetry {
					break
				}
				_ = flushSync(t, p)
				time.Sleep(2 * time.Millisecond)
			}

			st := ms.lastUpdateForTxid(f.target)
			if st != nil && st.Status == models.StatusRejected {
				t.Fatalf("row %s: a persistent %s was laundered into REJECTED %q", tc.row, tc.what, st.ExtraInfo)
			}
			if st != nil && st.Status == models.StatusPendingRetry && strings.TrimSpace(st.ExtraInfo) == "" {
				t.Fatalf("row %s: parked with no reason", tc.row)
			}
		})
	}
}
