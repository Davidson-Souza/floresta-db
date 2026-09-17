<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

## Intro

This is the original design plan for `floresta-db`, a specialized database intended to demonstrate high concurrency for Bitcoin transaction inputs and outputs. Threads cooperate without broad synchronization mechanisms such as mutexes. **Shared database state is synchronized with compare-and-swap (CAS) operations unless a documented exception applies.**

## How it works

We have four parts:
 - The blobs file (optional)
 - The body file
 - The heads file
 - The blk cout (one per file above)

The database uses separate chaining: `heads` stores one atomic root per bucket, while fixed-width nodes in `body` carry the next offset. The heads mapping is sized at startup and should balance resident memory against collision depth. All files are memory-mapped, with Linux advice favoring random access and retaining hot pages.

With the exception of the heads file, data files reserve a large stable virtual mapping but begin with only their header page. A tagged CAS high-water mark extends the backing file one block at a time. Allocation must first pop the tagged LIFO free-list head; only an empty free list permits high-water growth.

The block-count file tracks each block's allocation cursor, live-object count, and lifecycle state. When the last object leaves a sealed block, the deleter writes the previous free-list head into that block and publishes the block by CASing the tagged head. Reused blocks retain their physical storage; no holes are punched.

Adding elements requires:
 - Allocate space inside the blobs file if it's a map — we should also support a set where the blobs file isn't used
 - Copy everything needed to the blobs file
 - Create a body node by allocating space in the body file, hashing the key, and selecting a bucket by taking the hash modulo the map size. Point it to the current map head and, for maps, to the blob entry associated with the data.
 - CAS the head to make this node visible

This database is **eventually consistent**: readers might see an **older state**, but they should **never** see an **invalid state**. We achieve this by:
 - Making incomplete or invalid states invisible to others — this is why we fill everything up before making it visible inside the map
 - Whenever something becomes visible, we use an atomic CAS for that.

Deleting an element CAS-unlinks the uniquely owned node and immediately decrements its block counts. The caller guarantees that no competing deletion targets the same live key and that no reader or checkpoint retains an affected bucket offset during removal.

## Stack

This should be written in Rust, and use xxHash as the hash function. No dependencies are allowed, it should focus on linux-based systems for now. You should test everything with:
 - Functional and unit tests
 - Fuzz
 - Miri
 - Valgrind

Avoid unsafes whenever possible, make small contained functions that abstracts the unsafes and build from there
Errors should always be treated and propagated, you are not allowed to panic.

## Evaludation

We then need to evaluate the final work by running some stress-test. The goal here is to have several concurrent threads adding and removing things from the database as fast as they can. We need to mimic how bitcoin's UTXOs work, where they have 70-80% chance of being spent within 100 blocks. After running these tests, you should plot everything in some nice charts for further study.
