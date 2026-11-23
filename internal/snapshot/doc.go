// Package snapshot creates, stores, streams and installs state-machine
// snapshots (D-14, docs/storage-design.md section 4).
//
// A snapshot is a Pebble checkpoint (hard-linked SSTs plus the MANIFEST) taken
// between apply batches, so its appliedIndex is exactly the node's applied
// index, plus a snapshot.meta file: the {index, term, membership,
// format_version, checksum} metadata and the CRC32C of every file. The session
// table lives in Pebble too (prefixes s and t), so it travels with the data.
//
// Layout under the data directory (storage-design 4.3, 7):
//
//	snap/<index:016x>-<term:016x>/   complete snapshots (the 2 newest are kept)
//	snap/tmp-<index>-<term>/         a snapshot being created (removed on start)
//	snap/incoming/<index>-<term>/    an inbound transfer (removed on start)
//	sm/CURRENT, sm/db-<index>/       the live database (see internal/storage)
//
// Transfers stream the files in 1 MiB chunks with a CRC per chunk and per
// file. There is no resume: a transfer that breaks starts over (D-14).
// Installing renames the staged directory into sm/ and atomically swaps
// sm/CURRENT, so a crash at any step leaves the old state or the new one.
package snapshot
