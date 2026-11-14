Internal

- Codec sources borrow a contiguous span with one lease read and remember the last segment they read, so name and string checks no longer rebuild and revalidate the stored record span for every short read.
