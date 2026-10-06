# Design notes

- [Transaction-log mapping capacity](src/binding/transaction_log/DESIGN.md) — fixed reader capacity, first-append publication, and rotation.
- [Verification table](src/binding/core/DESIGN.md): slots store versions encoded with a per-key tag; a park cancels its lock wake registration when it ends.
