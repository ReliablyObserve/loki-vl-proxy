package logql

import "sync"

// Budget is a planning budget a caller hands the validation (queryplan.Budget has the same method): Exhausted reports
// that the time the caller allows for its planning has run out. The validation asks it while it parses and walks the
// tree, and stops.
type Budget interface{ Exhausted() bool }

// The validation asks the budget only after budgetFirstCheck steps (tokens read, nodes walked), and then every
// budgetCheckEvery steps: a query of fewer steps is never interrupted, so its verdict (Loki's rejection included) is
// always made, and a clock read per token would cost more than the token.
const (
	budgetFirstCheck = 1024
	budgetCheckEvery = 256
)

// budgetCheck is the budget of one validation, shared by every parser and scanner it makes (a quiet re-read of a call,
// a lookahead copy) and by its walk of the tree: once tripped, every later step reports it, and each scanner answers
// the end of its text.
type budgetCheck struct {
	budget Budget
	// steps counts the steps; next is the step of the next question (0 once tripped, so every step asks).
	steps, next uint32
	tripped     bool
}

// budgetChecks keeps the checks of finished validations: a validation under a budget takes one (newBudgetCheck) and
// gives it back when it returns (release), so asking costs it no allocation. Nothing a validation returns keeps a
// parser or a scanner, so no check outlives its validation.
var budgetChecks = sync.Pool{New: func() any { return new(budgetCheck) }}

// newBudgetCheck is the check of budget, nil for no budget; the caller releases it when its validation returns.
func newBudgetCheck(budget Budget) *budgetCheck {
	if budget == nil {
		return nil
	}
	c := budgetChecks.Get().(*budgetCheck)
	*c = budgetCheck{budget: budget, next: budgetFirstCheck}
	return c
}

// release gives the check back once its validation has returned.
func (c *budgetCheck) release() {
	if c == nil {
		return
	}
	*c = budgetCheck{}
	budgetChecks.Put(c)
}

// step counts one step and reports whether the budget has run out. It is small enough to inline: the budget is
// asked (ask) only at the steps it is due.
func (c *budgetCheck) step() bool {
	if c == nil {
		return false
	}
	c.steps++
	return c.steps >= c.next && c.ask()
}

// ask asks the budget at a step it is due and schedules the next question.
func (c *budgetCheck) ask() bool {
	if !c.tripped {
		c.tripped = c.budget.Exhausted()
		c.next = c.steps + budgetCheckEvery
	}
	if c.tripped {
		c.next = 0
	}
	return c.tripped
}

// exhausted reports a check that has tripped.
func (c *budgetCheck) exhausted() bool { return c != nil && c.tripped }
