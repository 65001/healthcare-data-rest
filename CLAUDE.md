# Working in this repo

## Testing

This is a Cargo workspace with multiple crates (`backend`, `cms-ingest`, `geo-enrich`, `compliance-probe`). When verifying a change, run tests scoped to the crates actually touched — e.g. `cargo test -p backend -p cms-ingest` — rather than `cargo test --workspace`. Re-testing untouched crates wastes time for no verification benefit. `cargo check --workspace` is still fine (and worth doing) since it's cheap and catches cross-crate wiring breaks; it's the full *test* suite specifically that should stay scoped.
