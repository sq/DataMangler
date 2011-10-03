Data Mangler
============

What is it?
-----------

It's a persistent embedded key-value store for .NET.

Wait, really? Why?
------------------

I needed a fast storage solution for a data-crunching application I was writing. I tried SQLite, but it was way too slow. All of the key-value stores out there were either under incompatible open-source licenses or required you to set up a server and send queries over the network.

How is it different?
--------------------
It uses memory-mapped files instead of handling disk I/O and caching itself. This ends up being a pretty good choice because while it's possible to do caching better than the kernel, it takes a lot more time and knowledge than most programmers have at their disposal. This also means you don't have to tune caching parameters to avoid running out of memory; the kernel figures out when to throw out cached pages.

It's designed to be embedded in a .NET application. The overhead of marshalling requests over the network to your storage server is gone, and so is the overhead of converting types from the .NET type system to whatever type system your storage engine uses.

It's type-safe. Each <tt>Tangle</tt> you create can only store values of a specific type (known at creation time) which means that you get the full benefits of the .NET type system when interacting with the datastore. In those edge cases where you really do need variants, you can still get them - just store <tt>object</tt>s.

Keys are arbitrary sequences of bytes instead of strings. So if you want to use an integer or a float as a key, you can.

Read operations against a <tt>Tangle</tt> are executed in parallel and you can interact with multiple <tt>Tangle</tt>s at once safely because they use separate storage streams. Write operations are sequential and cannot occur at the same time as read operations. This makes it a bad fit for servers with thousands of users, but the lack of locking and synchronization overhead means it is blazing fast for single-user cases, and it still makes great use of multiple cores.

Your dataset doesn't have to fit in memory. Despite using memory-mapped files, it only maps reasonably-sized chunks of the database into memory at any given time, so you can use it as a part of a larger application without completely exhausting your address space even if you're storing gigabytes of data.