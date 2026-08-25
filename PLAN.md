## Intro

This is a super scalar database experiment, our goal is to prove that we can accieve a high level of concurrency using a specially designed database for a specific problem. We are interested in indexing Bitcoin-related data, such as transaction inputs/outputs. The database must allow for concurrent threads working toghether, with no strong syncronization mechanism being used. That means no mutex, fence or anything that would otherwise severely disrupt other threads. **The only syncronization technique allowed here is a Compare and Swap (CAS) operation**, anything else can only be used if explicitely allowed.

## How it works

We have four parts:
 - The blobs file (optional)
 - The body file
 - The heads file
 - The blk cout (one per file above)

This database will work as a linear probing hash map, the heads are the bucket heads. This file must be small enough to fit in memory, but not too small to create a huge collision rate. We don't re-hash within the same session, so it must be allocated to a big size at startup. The body file will hold the excess buckets, they will be organized as linked lists, each pointer will be a position inside the body file. All files are memory-mapped. You should use every single bit of API the linux kernel gives you (don't touch sysctls though) to ask for the kernel to **not reclaim that space unless it's really needed, and to be very lazy on flushing, waiting until there's a big chunk of things to flush**. Things must live in memory for as long as we can, to improve performance.

With the exception of the heads file, all files will grow. We should however, allocate a pretty big sparce file to avoid needing to call resize every time on them. Threads allocate space by changing a pointer that points to the first free position inside the file. They should CAS it and retry until they succeed. They might batch allocate for say, and entire block, but never overcommit without needing. This could hurt locality.

The blk cout will track blocks of N bytes, and how many objects there are. When deleting something, you must remove that from the map and decrease this counter the respective block, also using CAS. If a block count goes to zero, you must open a hole there, giving this space back to the system.

Adding elements requires:
 - Allocate space inside the blobs file if it's a map — we should also support a set where the blobs file isn't used
 - Copy everything needed to the blobs file
 - Create a body node for it by allocating a node inside the body file, hash the key, find a bucket by modulu-ing with the map size. Make it point to what the map's head is currently pointing to. It should also point to the blobs entry associate with that data, if any.
 - CAS the head to make this node visible

This database is **eventually consistent**, this means you might see an **older state when reading it**, but you should **never** see an **invalid state**. We accieve this by:
 - Making incomplete or invalid states invisible to others — this is why we fill everything up before making it visible inside the map
 - Whenever something becomes visible, we use an atomic CAS for that.

Deleting an element must also use CAS to update the bucket head or predecessor pointer that points to the deleted body node.

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
