# Hirebotics TypeORM fork

Fork of TypeORM consumed by beacon3's packages/cloud-connector (sqlite and better-sqlite3)
and packages/server (postgres). Support only those drivers and Node.
Do not add compatibility for runtimes or libraries the consumers do not use.

## Fork boundaries

- Custom logic lives in fork-owned files. Upstream files get only minimal, surgical edits.
- Every fork edit inside an upstream file carries a comment starting `Hirebotics patch:`
  that states why the change exists and what to preserve when merging upstream.
- Every fork-owned file in `src/` states `Hirebotics file, not part of upstream TypeORM.`
  in its header.
- Shared sqlite shapes live in `src/driver/sqlite-abstract/sqlite.types.ts`.
- Each driver's query runner lives in that driver's folder.

## Review the whole, then the parts

- Before editing, and again before declaring any review or refactor done,
  read the entire fork delta as one system, not as diff hunks.
- Judge relationships, not items in isolation:
  identifiers that form pairs, logic duplicated across files, callers across files,
  consumers of every output, and lifecycle symmetry (acquire/release, open/close).
- Every local edit gets a follow-up question: what else in the system
  referenced, mirrored, or depended on the thing just changed?
- A finding the user can spot in one glance at the whole file
  means the review was too narrow, not too strict.

## Simplification (apply while writing, not on request)

- Justify every method, field, type, and constant by caller count or the concept it names.
  One caller and no concept means inline it.
  After any refactor, re-count callers: helpers left with one caller get inlined.
- Any expression or condition that appears more than once becomes one named thing.
- Ask: a from-scratch rewrite that passes the tests would not contain which lines?
  Delete those lines.
- Every log line, metric, and diagnostic must name a real consumer that exists today.
  No consumer means delete it. Hirebotics has no alerting on warn logs.
- No speculative generality: no options, branches, or fallbacks for cases no consumer hits.
- Prefer a wall-clock deadline over an attempt count when bounding retries or waits.

## Naming

- Booleans start with `is`, `has`, or `can`.
- Numeric counts end in `Count`; durations end in `Ms`.
- Accessor methods start with `get` (ES `get` accessors are fine as-is).
- Factory functions start with `create` or `make`.
- Identifiers created as a pair mirror each other's structure
  (`TRANSACTION_BEGIN_STATEMENT` / `TRANSACTION_END_STATEMENT`).
- Names state purpose in plain domain words. No invented jargon
  (banned examples: "slot", "on the wire", "load-bearing", "vacuously", "barge").
- Shared sqlite types carry the `Sqlite` prefix.
- Match upstream TypeORM conventions where they exist (it uses get-prefixed accessors).

## Style

- No ternary operators. Use if/else, even for one-liners.
- Braces on every if/else/for/while body and every arrow function body.
- async/await over .then/.catch. Exception: tests that record promise resolution
  order need .then, with a one-line reason comment.
- No `any` on introduced code. Type the shape of every parameter and field.
  `any` is allowed only to match an upstream signature being overridden.
  Confine an unavoidable cast to one documented place.

## Comments

- Comments state why, or a contract, or a constraint the code cannot show. Nothing else.
- The strongest why-comment names the failure that occurs if the code changes:
  state what breaks and its consequence, not the stylistic choice.
- Never reference how other code calls or uses this code. State the contract instead.
- Each sentence starts its own line. Never wrap a sentence mid-thought.
  Two sentences may share a line only if they fit without wrapping.
  A sentence too long for one line gets rewritten shorter, not wrapped.
- Short sentences, active voice, plain words, no semicolons, no analogies.
- If deleting or shortening a comment loses nothing critical, delete or shorten it.

## Verification

- Node 20 is required (`better-sqlite3@8.7.0` does not build on Node 22+). Use nvm.
- Run `./beacon/test.sh`, never `pnpm test` directly (see beacon/README.md).
- For a fast loop: compile, then run the four abstract-sqlite test files with mocha
  and `beacon/mocharc.beacon.json` after swapping in `beacon/ormconfig.beacon.json`.
- Before declaring done: prettier, eslint (0 errors), `tsc --noEmit`, tests green.
- Do not commit or push unless asked.
