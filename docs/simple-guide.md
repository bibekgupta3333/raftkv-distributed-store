# RaftKV, Explained Simply

*No jargon needed. If you want the deep technical details, see the other files in this folder.*

---

## What is it?

RaftKV is a **database that doesn't lose your data, even when computers break.**

It stores simple pairs, like a dictionary:

```
"user:42:name"  →  "Asha"
"cart:42"       →  "3 items"
```

You can **save** a value, **read** it back, **delete** it, or **update it only if it hasn't changed**.

The trick: it runs on **5 computers at once**, and they all keep the same copy.

---

## Why 5 computers?

Because computers fail. Disks die, power goes out, networks get cut.

With 5 copies, RaftKV keeps working as long as **any 3 are healthy**.
Two can be broken at the same time and nothing is lost.

| Computers working | What happens |
|---|---|
| 5 of 5 | Everything normal |
| 4 of 5 | Still works, nobody notices |
| 3 of 5 | Still works |
| 2 of 5 | Pauses rather than risk wrong answers. Resumes once a third computer comes back. |

**Rule of thumb:** it would rather pause than lie.

---

## How does it work?

### 1. One leader is chosen

The 5 computers vote and pick one **leader**. All writes go through the leader.
The other four are **followers**.

### 2. Saving a value

```
You ──"save X"──▶ Leader
                    │
                    ├─ writes it to its own disk
                    ├─ sends it to the 4 followers
                    │
                    ▼
         Once 3 computers (counting itself) have it on disk
                    │
                    ▼
You ◀──── "OK, saved" ────
```

Once you see **"OK"**, the value is safe. Even if the leader crashes one second later, the others have it.

### 3. Reading a value

Before answering, the leader double-checks with the followers: *"Am I still the leader?"*
That way you never get an old value from a leader that was quietly replaced.

### 4. When the leader dies

The followers notice the leader went quiet (after about 1–2 seconds) and **vote for a new one**.
The rules make sure the new leader always has every saved value. Apps just retry and carry on.

### 5. Keeping things tidy

- Every change is written to a **log**, like a diary. The log can grow forever, so each computer regularly takes a **snapshot** (a photo of the current data) and throws away the old diary pages.
- A computer that was offline for a long time downloads the latest snapshot instead of replaying months of diary.
- You can **add or remove computers** while everything keeps running.

---

## What are we promising?

1. **No lost writes.** If you got "OK", the data survives crashes.
2. **No stale reads.** You always see the latest saved value.
3. **No double writes.** If your app retries a save because of a network hiccup, it only happens once.
4. **Keeps running** with up to 2 of 5 computers down.

---

## How will we know it actually works?

Saying it works is easy. Proving it is the real project, and the best part to learn from.

We **break it on purpose**, over and over:

- Kill the leader in the middle of a write
- Freeze a computer, then unfreeze it
- Cut the network so the computers split into groups
- Make the network slow and lossy
- Restart a computer and watch it catch up

While that happens, we record every operation and run a **checker program** that looks for even one wrong answer.
**Pass = zero wrong answers.**

A quick version runs on GitHub for every code change, and a longer one runs every night.
You can also watch it live in the **observer page** in your browser.

---

## What is it built with?

| Piece | What it does |
|---|---|
| **Go** | The programming language |
| **Raft** | The voting and copying rules (written ourselves) |
| **Pebble** | Stores the data on each computer's disk |
| **gRPC** | How apps and computers talk to each other |
| **Docker / Kubernetes (kind)** | Runs the 5 "computers" on your laptop |
| **Prometheus + OpenTelemetry** | Metrics and tracing, to see inside |
| **GitHub Actions** | Runs all the tests automatically |

---

## The plan

| Step | What gets built |
|---|---|
| M0 | Project setup |
| M1 | Voting and copying rules, plus a simulator to test them |
| M2 | Real disk log and real networking (first time it runs on your laptop) |
| M3 | Storing the actual data |
| M4 | Safe reads and no double writes |
| M5 | Snapshots and catching up |
| M6 | Adding and removing computers |
| M7 | Kubernetes, the observer page, and break-it tests |

---

*Status: planning. No code written yet. Speed is not a goal; this is a learning project.*
