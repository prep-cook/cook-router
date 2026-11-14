use std::{sync::Arc, task::Poll};

use ck_stream::{FRAME_BYTES, Lease, ManualClock, Read, Store, Writer};

use super::{Source, Span};
use crate::codec::{Error, json};

struct Fixture {
    clock: Arc<ManualClock>,
    store: Store,
    writer: Writer,
    directory: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("ck-source-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(8 * FRAME_BYTES);
        config.max_event_bytes = FRAME_BYTES;
        config.header_slots = 4;
        config.workers = 1;
        let clock = Arc::new(ManualClock::default());
        let store = Store::with_clock(config, clock.clone()).unwrap();
        let writer = store.create().unwrap();
        Self {
            clock,
            store,
            writer,
            directory,
        }
    }

    /// Publishes `filler` then `record`, and returns the lease of `record`.
    fn lease(&mut self, filler: usize, record: &[u8]) -> Lease {
        self.writer.append(&vec![b'x'; filler]).unwrap();
        self.writer.append(record).unwrap();
        let mut reader = self.store.reader(self.writer.id(), 0).unwrap();
        let mut last = None;
        while let Poll::Ready(Some(Read::Record(lease))) = reader.try_next() {
            last = Some(lease);
        }
        last.unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.store.shutdown().unwrap();
        std::fs::remove_dir(&self.directory).unwrap();
    }
}

fn span(start: usize, end: usize) -> Span {
    Span {
        start: start as u64,
        end: end as u64,
    }
}

#[test]
fn empty_spans_borrow_nothing_up_to_the_exclusive_end() {
    let mut fixture = Fixture::new();
    let mut source = Source::new(&mut fixture.writer).unwrap();
    source.push_bytes(&mut fixture.writer, b"abc").unwrap();
    for offset in 0..=3 {
        assert_eq!(source.slice(span(offset, offset)).unwrap(), Some(&[][..]));
    }
    assert!(matches!(source.slice(span(4, 4)), Err(Error::Position)));
    assert!(matches!(source.slice(span(2, 1)), Err(Error::Position)));
}

#[test]
fn a_span_ending_at_a_segment_boundary_is_contiguous() {
    let mut fixture = Fixture::new();
    let mut source = Source::new(&mut fixture.writer).unwrap();
    let bytes: Vec<u8> = (0..FRAME_BYTES + 10).map(|i| b'a' + (i % 26) as u8).collect();
    source.push_bytes(&mut fixture.writer, &bytes).unwrap();
    let end = FRAME_BYTES;
    assert_eq!(source.slice(span(end - 4, end)).unwrap(), Some(&bytes[end - 4..end]));
    assert_eq!(source.slice(span(end, end + 10)).unwrap(), Some(&bytes[end..]));
    assert_eq!(source.slice(span(end - 4, end + 1)).unwrap(), None);
    assert!(source.equals(span(end - 4, end + 1), &bytes[end - 4..end + 1]).unwrap());
}

#[test]
fn a_short_record_straddling_two_frames_takes_the_fallback() {
    let mut fixture = Fixture::new();
    let lease = fixture.lease(FRAME_BYTES - 6, br#""abcdefghij""#);
    let record = lease.record();
    assert_eq!(record.start.frame + 1, record.end.frame);
    let mut source = Source::new(&mut fixture.writer).unwrap();
    source.push(lease, record.start..record.end).unwrap();
    assert_eq!(source.slice(span(0, 6)).unwrap(), Some(&br#""abcde"#[..]));
    assert_eq!(source.slice(span(0, 12)).unwrap(), None);
    assert!(json::string_is(&source, span(0, 12), "abcdefghij").unwrap());
    assert!(!json::string_is(&source, span(0, 12), "abcdefghik").unwrap());
    assert!(matches!(json::Decoded::new(&source, span(0, 11)), Err(Error::Json)));
}

#[test]
fn borrows_after_invalidation_fail() {
    let mut fixture = Fixture::new();
    let lease = fixture.lease(4, br#""name""#);
    let record = lease.record();
    let mut source = Source::new(&mut fixture.writer).unwrap();
    source.push(lease, record.start..record.end).unwrap();
    assert!(json::string_is(&source, span(0, 6), "name").unwrap());
    fixture.clock.advance(std::time::Duration::from_secs(3601));
    fixture.store.tick().unwrap();
    assert!(matches!(
        source.slice(span(0, 6)),
        Err(Error::Storage(ck_stream::Error::Gone))
    ));
    assert!(matches!(
        json::string_is(&source, span(0, 6), "name"),
        Err(Error::Storage(ck_stream::Error::Gone))
    ));
}

#[test]
fn releasing_segments_resets_the_recent_segment() {
    let mut fixture = Fixture::new();
    let mut source = Source::new(&mut fixture.writer).unwrap();
    let bytes: Vec<u8> = (0..3 * FRAME_BYTES).map(|i| (i / FRAME_BYTES) as u8).collect();
    source.push_bytes(&mut fixture.writer, &bytes).unwrap();
    let last = span(2 * FRAME_BYTES, 2 * FRAME_BYTES + 1);
    assert_eq!(source.slice(last).unwrap(), Some(&[2][..]));
    assert_eq!(source.recent.get(), 2);
    source.release_before(FRAME_BYTES as u64).unwrap();
    assert_eq!(source.recent.get(), 0);
    assert_eq!(source.slice(last).unwrap(), Some(&[2][..]));
    assert_eq!(source.recent.get(), 1);
    source.release_range(span(FRAME_BYTES, 2 * FRAME_BYTES)).unwrap();
    assert_eq!(source.recent.get(), 0);
    assert_eq!(source.slice(last).unwrap(), Some(&[2][..]));
}
