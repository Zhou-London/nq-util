//! Framing for the nlib wire records, and the conversions Kraken's JSON needs
//! to reach them: fixed-point decimals, RFC3339 timestamps, and the hashes
//! that map Kraken's string ids onto the wire's integers.
//!
//! A frame is nqbook's one tag byte followed by the record's own bytes in
//! host layout. The records come from `common.h` through [`crate::nlib`], so
//! the layout on the wire is the C++ struct's.

use crate::nlib;

/// Fixed-point decimals, read off nlib's scales.
pub const PRICE_DECIMALS: i32 = nlib::price_scale.ilog10() as i32;
pub const QTY_DECIMALS: i32 = nlib::qty_scale.ilog10() as i32;

/// A record nqbook's `RunFeed` decodes, with the frame tag that selects it —
/// `kOrderTag` and `kTradeTag` in nqbook's `Pipeline.h`.
pub trait Record: Copy {
    const TAG: u8;
}

impl Record for nlib::order {
    const TAG: u8 = 0;
}

impl Record for nlib::trade {
    const TAG: u8 = 1;
}

/// Buffer sized for the larger record, so one frame type carries both.
const CAPACITY: usize = 1 + if size_of::<nlib::order>() > size_of::<nlib::trade>() {
    size_of::<nlib::order>()
} else {
    size_of::<nlib::trade>()
};

/// One framed record, ready for the PUB socket.
#[derive(Clone, Copy)]
pub struct Frame {
    bytes: [u8; CAPACITY],
    len: usize,
}

impl Frame {
    /// Frames `record` byte for byte behind its tag. nlib's records are
    /// trivially copyable and standard layout — `common.h` asserts both — so
    /// their object representation is the wire image.
    pub fn new<T: Record>(record: T) -> Self {
        let mut frame = Self {
            bytes: [0; CAPACITY],
            len: 1 + size_of::<T>(),
        };
        frame.bytes[0] = T::TAG;
        // SAFETY: the buffer is sized for the larger record, so `T` fits past
        // the tag byte; the write is unaligned, so the tag's offset places no
        // constraint on it.
        unsafe {
            std::ptr::write_unaligned(frame.bytes.as_mut_ptr().add(1).cast::<T>(), record);
        }
        frame
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// Maps a Kraken order id — a base32 string wider than 64 bits — to the
/// wire's int64 as its FNV-1a hash. Stateless, so an id maps identically
/// across connections, reconnects and restarts; collisions are accepted (at
/// one million resting orders the chance of any is about 5e-8).
pub fn order_id(id: &str) -> i64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for &byte in id.as_bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(PRIME);
    }
    hash as i64
}

/// Maps a websocket pair name to the wire's instrument id as its 32-bit
/// FNV-1a hash. Stateless for the same reason as [`order_id`];
/// `--list-symbols` prints the mapping for joining stored data back to names.
pub fn instrument_id(symbol: &str) -> u32 {
    const OFFSET: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;
    let mut hash = OFFSET;
    for &byte in symbol.as_bytes() {
        hash = (hash ^ u32::from(byte)).wrapping_mul(PRIME);
    }
    hash
}

/// Parses a JSON decimal (optional sign, fraction, exponent) into fixed point
/// with `decimals` fractional digits, rounding half away from zero. Exact —
/// the text never round-trips through a float. None on malformed text or a
/// value outside i64.
pub fn scaled(text: &str, decimals: i32) -> Option<i64> {
    let bytes = text.as_bytes();
    let mut i = 0;
    let negative = match bytes.first()? {
        b'-' => {
            i += 1;
            true
        }
        b'+' => {
            i += 1;
            false
        }
        _ => false,
    };

    let mut mantissa: i128 = 0;
    let mut digits = 0;
    let mut frac_digits: i32 = 0;
    let mut in_fraction = false;
    while let Some(&byte) = bytes.get(i) {
        match byte {
            b'0'..=b'9' => {
                mantissa = mantissa.checked_mul(10)?.checked_add(i128::from(byte - b'0'))?;
                digits += 1;
                if in_fraction {
                    frac_digits += 1;
                }
            }
            b'.' if !in_fraction => in_fraction = true,
            b'e' | b'E' => break,
            _ => return None,
        }
        i += 1;
    }
    if digits == 0 {
        return None;
    }

    let mut exponent: i32 = 0;
    if let Some(b'e' | b'E') = bytes.get(i) {
        exponent = text.get(i + 1..)?.parse().ok()?;
    }

    // The scaled value is mantissa * 10^shift; a negative shift means the
    // text carries more precision than the wire and is rounded half away
    // from zero.
    let shift = decimals - frac_digits + exponent;
    let value = if shift >= 0 {
        mantissa.checked_mul(10i128.checked_pow(u32::try_from(shift).ok()?)?)?
    } else {
        let divisor = 10i128.checked_pow(u32::try_from(-shift).ok()?)?;
        (mantissa + divisor / 2) / divisor
    };
    let value = if negative { -value } else { value };
    i64::try_from(value).ok()
}

/// Parses an RFC3339 timestamp with up to nanosecond precision into
/// Unix-epoch nanoseconds; None if malformed or outside the i64 range.
pub fn event_ns(timestamp: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .ok()?
        .timestamp_nanos_opt()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads a record back out of the frame it was written into.
    fn decode<T: Record>(frame: &Frame) -> T {
        assert_eq!(frame.as_bytes().len(), 1 + size_of::<T>());
        assert_eq!(frame.as_bytes()[0], T::TAG);
        // SAFETY: the frame holds exactly one `T` past the tag byte, checked
        // above; the read is unaligned for the same reason the write is.
        unsafe { std::ptr::read_unaligned(frame.as_bytes()[1..].as_ptr().cast::<T>()) }
    }

    #[test]
    fn order_frames_carry_every_field() {
        let order = nlib::order {
            seq: 1,
            order_id: 2,
            price: 3,
            qty: 4,
            cancel_qty: 5,
            new_qty: 6,
            event_ns: 7,
            prev: std::ptr::null_mut(),
            next: std::ptr::null_mut(),
            instrument_id: 8,
            side: nlib::side::sell,
            type_: nlib::order_type::limit,
            action: nlib::order_action::modify,
            recv_ns: 9,
        };
        let decoded: nlib::order = decode(&Frame::new(order));
        assert_eq!(decoded.seq, 1);
        assert_eq!(decoded.order_id, 2);
        assert_eq!(decoded.price, 3);
        assert_eq!(decoded.qty, 4);
        assert_eq!(decoded.cancel_qty, 5);
        assert_eq!(decoded.new_qty, 6);
        assert_eq!(decoded.event_ns, 7);
        assert_eq!(decoded.instrument_id, 8);
        assert_eq!(decoded.side, nlib::side::sell);
        assert_eq!(decoded.type_, nlib::order_type::limit);
        assert_eq!(decoded.action, nlib::order_action::modify);
        assert_eq!(decoded.recv_ns, 9);
    }

    #[test]
    fn trade_frames_carry_every_field() {
        let trade = nlib::trade {
            seq: 1,
            buy_order_id: 2,
            sell_order_id: 3,
            price: 4,
            qty: 5,
            event_ns: 6,
            instrument_id: 7,
            side: nlib::side::buy,
            recv_ns: 8,
        };
        let decoded: nlib::trade = decode(&Frame::new(trade));
        assert_eq!(decoded.seq, 1);
        assert_eq!(decoded.buy_order_id, 2);
        assert_eq!(decoded.sell_order_id, 3);
        assert_eq!(decoded.price, 4);
        assert_eq!(decoded.qty, 5);
        assert_eq!(decoded.event_ns, 6);
        assert_eq!(decoded.instrument_id, 7);
        assert_eq!(decoded.side, nlib::side::buy);
        assert_eq!(decoded.recv_ns, 8);
    }

    #[test]
    fn decimals_follow_nlib_scales() {
        assert_eq!(PRICE_DECIMALS, 10);
        assert_eq!(QTY_DECIMALS, 8);
    }

    #[test]
    fn fnv1a_reference_vectors() {
        assert_eq!(order_id("") as u64, 0xcbf29ce484222325);
        assert_eq!(order_id("a") as u64, 0xaf63dc4c8601ec8c);
        assert_eq!(instrument_id(""), 0x811c9dc5);
        assert_eq!(instrument_id("a"), 0xe40c292c);
    }

    #[test]
    fn scaled_is_exact_fixed_point() {
        assert_eq!(scaled("104561.7", PRICE_DECIMALS), Some(1_045_617_000_000_000));
        assert_eq!(scaled("0.0000012345", PRICE_DECIMALS), Some(12_345));
        assert_eq!(scaled("0.00000001", QTY_DECIMALS), Some(1));
        assert_eq!(scaled("42", QTY_DECIMALS), Some(4_200_000_000));
        assert_eq!(scaled("-1.5", 2), Some(-150));
        assert_eq!(scaled("1e-7", 8), Some(10));
        assert_eq!(scaled("1.2E2", 0), Some(120));
    }

    #[test]
    fn scaled_rounds_excess_precision_half_away_from_zero() {
        assert_eq!(scaled("0.125", 2), Some(13));
        assert_eq!(scaled("-0.125", 2), Some(-13));
        assert_eq!(scaled("0.124", 2), Some(12));
    }

    #[test]
    fn scaled_rejects_garbage_and_overflow() {
        assert_eq!(scaled("", 2), None);
        assert_eq!(scaled(".", 2), None);
        assert_eq!(scaled("1.2.3", 2), None);
        assert_eq!(scaled("abc", 2), None);
        assert_eq!(scaled("1e30", 0), None);
        assert_eq!(scaled("999999999999", PRICE_DECIMALS), None); // > i64 at 1e10
    }

    #[test]
    fn event_ns_parses_kraken_timestamps() {
        assert_eq!(
            event_ns("2023-10-06T17:35:55.440295Z"),
            Some(1_696_613_755_440_295_000)
        );
        assert_eq!(
            event_ns("2023-10-06T18:20:56.506266789Z"),
            Some(1_696_616_456_506_266_789)
        );
        assert_eq!(event_ns("not a time"), None);
    }
}
