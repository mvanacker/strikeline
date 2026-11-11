# Strikeline TODO

What stands between today's testnet desk and anything that holds real money, plus the gaps in the testnet build itself. The first section is a hard gate: nothing below it matters for real funds until it is cleared.

## Legal and compliance (gate for any real money)

- **Legal review before real funds.** Retail binary options are banned or restricted in many jurisdictions (EU/UK retail ban; US only on CFTC-registered exchanges; gambling licensing elsewhere). Holding user crypto is custody / money transmission (FinCEN MSB, MiCA CASP). Get a written opinion for every jurisdiction served.
- **Identity checks and AML.** KYC before deposits; sanctions screening of deposit senders and withdrawal addresses (OFAC list, chain-analytics provider); transaction monitoring; Travel Rule for transfers over the threshold; suspicious activity reporting.
- **Terms, age and responsible play.** Terms of service, risk disclosure, age verification, deposit and loss limits, self-exclusion, cooling-off.
- **Geo-restriction.** Block jurisdictions where the product is not allowed.

## Custody and keys

- **Keys out of plaintext files.** The hot-wallet key (`data/desk_key.txt` or `STRIKELINE_DESK_KEY`) and the deposit-address seed (`data/deposit_seed.txt` or `STRIKELINE_DEPOSIT_SEED`) are fine for testnet only. Move signing to a KMS/HSM or an MPC custody provider; nothing on the app host should be able to sign alone.
- **Back up the deposit seed.** Every user's deposit address derives from it; if it's lost, anything not yet swept is stranded. Keep an offline copy and test restoring it.
- **Hot/cold split.** Keep only a float in the hot wallet, sized to a day of withdrawals; refill from cold storage by a separate, approved process.
- **Gas top-ups and balance alerts.** Alert when the desk's ETH or USDC runs low; withdrawals currently just wait with a "topped up" note.

## Ledger and reconciliation

- **Full double-entry ledger.** Only deposits, withdrawals and refunds are in `ledger`. Trades, settlements, sales and resets change the book JSON without ledger rows; record every balance change with a counter-account (desk P&L, user cash, in-flight withdrawals).
- **Daily reconciliation.** On-chain desk balance vs. sum of user cash + queued withdrawals; alert on any difference. Solvency check before each withdrawal batch.
- **Money as integers.** Balances are floats; move to integer micro-USDC everywhere.

## Deposits

- **Reorgs and finality.** Credit after 10 L2 blocks. For real money, wait for L1 finality of the Base batch (or a provider's finality signal) and handle a credited deposit disappearing.
- **Other tokens sent to a deposit address.** Only USDC is watched. ETH or other tokens sent by mistake sit in the deposit address unnoticed; add a way to detect and return them.
- **Batch balance reads.** The solvency check reads each funded deposit address with its own call every few seconds; switch to a multicall contract (or a provider's batch API) before there are many users.
- **Small deposits are never swept.** Addresses holding less than 1 USDC aren't swept (not worth the gas) and are re-checked on every pass; sweep them in batches or on a slower schedule.- **More than one RPC.** One public RPC with a 500-block log limit; add a paid provider with failover and rate-limit handling.

## Withdrawals

- **Operator review queue.** Large or unusual withdrawals, and any in `review` status (nonce clash), need a console to inspect, approve, refund or mark sent, with an audit trail. There is no admin tool today.
- **Stuck transaction replacement.** A signed withdrawal is re-broadcast unchanged; if fees spike it can sit forever. Add fee bumping (replace by fee at the same nonce) with care not to double-pay.
- **Withdrawal confirmation and address allowlist.** Today a logged-in session can withdraw to any address straight away. Add confirmation by email or 2FA, an allowlist of saved addresses, and a hold on newly added ones, so a stolen session can't empty an account.
- **Batching and fees.** Who pays gas, minimums that cover it, batching small withdrawals.

## Accounts and security

- **HTTPS and Secure cookies.** Served over plain http on 127.0.0.1; set `secure=True` on the session cookie behind TLS.
- **Two-factor login.** TOTP or passkeys, required for withdrawals.
- **Password change, reset and email verification.** None exist yet; reset needs a verified email.
- **Session list and revoke.** Let users see and end their sessions; end all sessions on password change.
- **Persistent login throttling.** Failed-login counters are in memory and clear on restart; add per-account and per-IP limits that survive restarts, and a signup rate limit.
- **Account closure and data export.**

## Risk (the desk is the counterparty)

- **Finer exposure limits.** In place: 100 USDC max stake per contract, and the desk refuses any contract unless its on-chain USDC still covers every user balance, every open payout and every unsent withdrawal. Still missing: limits per user and per coin, and a configurable buffer above that hard floor.
- **Desk capital management.** Trading only works while the desk wallet holds its own USDC on top of user balances; add a way to add or withdraw desk capital and report it, instead of sending tokens to the address by hand.
- **Model risk.** The lognormal model under-prices jumps and fat tails; track realized vs. predicted hit rates per strike and expiry, and widen the edge or limits when it drifts.
- **Kill switch.** One switch that stops new contracts (and, per the fail-safe rule, keeps settling or voids open ones cleanly) when feeds, pricing or the ledger look wrong.
- **Desk P&L and hedging.** Report desk P&L by coin and expiry; decide whether to hedge large exposure on a venue.

## Operations

- **Backups and migrations.** SQLite file with no backups or schema migrations; add scheduled backups (and restore drills) and a migration tool, or move to Postgres before running more than one instance.
- **Monitoring and alerts.** Price feed down, IV stale, deposit scanner behind, withdrawal queue age, error rates, desk balances.
- **Audit log.** Who did what and when, for every admin and money-moving action.
- **Automated tests in the repo.** The funding and auth checks were run as throwaway scripts; turn them into a test suite, plus an integration run against a local chain (anvil) for deposits and withdrawals end to end.
