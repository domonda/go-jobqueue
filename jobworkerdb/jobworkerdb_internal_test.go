package jobworkerdb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/domonda/go-sqldb/pqconn"
)

// TestBuildClaimJobQuery verifies the StartNextJobOrNil claim query is assembled
// correctly with the registered job types inlined as SQL literals.
//
// It uses pqconn.QueryFormatter{} on purpose: that is the same QueryFormatter the
// production claim path reaches via db.Conn(ctx), so the literals are escaped here
// exactly as they will be at runtime. This is a pure string test and needs no
// database.
func TestBuildClaimJobQuery(t *testing.T) {
	formatter := pqconn.QueryFormatter{}

	t.Run("single job type", func(t *testing.T) {
		query := buildClaimJobQuery([]string{"email"}, formatter)

		// The static skeleton of the combined claim+update statement.
		assert.Contains(t, query, "with claimed as (")
		assert.Contains(t, query, "select id")
		assert.Contains(t, query, "from worker.job")
		assert.Contains(t, query, "where started_at is null")
		assert.Contains(t, query, "start_at <= now()")
		assert.Contains(t, query, `and "type" in ('email')`)
		assert.Contains(t, query, "order by")
		assert.Contains(t, query, "priority desc")
		assert.Contains(t, query, "created_at asc")
		assert.Contains(t, query, "limit 1")
		assert.Contains(t, query, "for update skip locked")
		assert.Contains(t, query, "update worker.job")
		assert.Contains(t, query, "returning worker.job.*")

		// The statement takes no bind parameters: job types are inlined and all
		// timestamps use now(), so it can be cached as a prepared statement.
		assert.NotContains(t, query, "$1")
		assert.NotContains(t, query, "$2")
	})

	t.Run("multiple job types are comma joined in slice order", func(t *testing.T) {
		query := buildClaimJobQuery([]string{"a", "b", "c"}, formatter)
		assert.Contains(t, query, `and "type" in ('a','b','c')`)
	})

	t.Run("single quotes are doubled so a payload cannot break out", func(t *testing.T) {
		jobType := `weird'); drop table worker.job; --`
		query := buildClaimJobQuery([]string{jobType}, formatter)

		// The inlined literal must equal the formatter's quoted form, which doubles
		// the single quote and keeps the whole payload inside one string literal.
		want := formatter.FormatStringLiteral(jobType)
		assert.Contains(t, query, "in ("+want+")")
		assert.Contains(t, want, "''", "single quote doubled by the formatter")
	})

	t.Run("backslashes switch to C-style E'' escaping", func(t *testing.T) {
		jobType := `back\slash`
		query := buildClaimJobQuery([]string{jobType}, formatter)

		want := formatter.FormatStringLiteral(jobType)
		assert.Contains(t, query, "in ("+want+")")
		assert.Contains(t, want, `E'`, "pq.QuoteLiteral uses E'...' when a backslash is present")
	})

	t.Run("inlined literals match formatter output exactly", func(t *testing.T) {
		jobTypes := []string{"plain", "with'quote", `with\backslash`}
		query := buildClaimJobQuery(jobTypes, formatter)

		quoted := make([]string, len(jobTypes))
		for i, jt := range jobTypes {
			quoted[i] = formatter.FormatStringLiteral(jt)
		}
		assert.Contains(t, query, "in ("+strings.Join(quoted, ",")+")")
	})
}
