//! The nlib wire image: `nlib::order` serialized byte for byte (little-endian
//! LP64) behind nqbook's one-byte frame tag, plus the rules that normalize
//! Kraken's level3 fields onto it. The layout mirrors nlib's `common.h`,
//! whose static_asserts pin the same 72-byte size; the tests here are the
//! Rust half of that contract.

/// Fixed-point decimals, matching nlib's `price_scale` and `qty_scale`.
pub const PRICE_DECIMALS: i32 = 10;
pub const QTY_DECIMALS: i32 = 8;

/// nqbook's frame tag for an order record.
const ORDER_TAG: u8 = 0;

/// One tag byte plus the 72-byte `nlib::order`.
pub const ORDER_FRAME_LEN: usize = 73;

/// One framed record, ready for the PUB socket.
pub type Frame = [u8; ORDER_FRAME_LEN];

/// `nlib::side` codes.
#[derive(Clone, Copy)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

/// `nlib::order_action` codes. `qty` means: for `Add`, the resting quantity;
/// for `Cancel`, the remaining quantity leaving the book; for `Modify`, the
/// new remaining quantity. `Clear` drops the instrument's resting orders
/// ahead of a snapshot replay.
#[derive(Clone, Copy)]
pub enum Action {
    Add = 0,
    Cancel = 1,
    Modify = 2,
    Clear = 3,
}

/// One normalized order event in `nlib::order` field order.
pub struct Order {
    pub seq: i64,
    pub order_id: i64,
    pub price: i64,
    pub qty: i64,
    pub event_ns: i64,
    pub instrument_id: u32,
    pub side: Side,
    pub action: Action,
}

impl Order {
    /// Encodes the frame nqbook's `RunFeed` decodes. The order type is always
    /// limit (only limit orders rest in a level3 book); the intrusive hooks
    /// and `recv_ns` stay zero — the hooks are book-owned and the receive
    /// time is stamped by the receiving process.
    pub fn encode(&self) -> Frame {
        let mut f = [0u8; ORDER_FRAME_LEN];
        f[0] = ORDER_TAG;
        f[1..9].copy_from_slice(&self.seq.to_le_bytes());
        f[9..17].copy_from_slice(&self.order_id.to_le_bytes());
        f[17..25].copy_from_slice(&self.price.to_le_bytes());
        f[25..33].copy_from_slice(&self.qty.to_le_bytes());
        f[33..41].copy_from_slice(&self.event_ns.to_le_bytes());
        f[57..61].copy_from_slice(&self.instrument_id.to_le_bytes());
        f[61] = self.side as u8;
        f[63] = self.action as u8;
        f
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

    #[test]
    fn encode_layout_matches_nlib_order() {
        let frame = Order {
            seq: 0x0102030405060708,
            order_id: 0x1112131415161718,
            price: 0x2122232425262728,
            qty: 0x3132333435363738,
            event_ns: 0x4142434445464748,
            instrument_id: 0x51525354,
            side: Side::Sell,
            action: Action::Modify,
        }
        .encode();
        assert_eq!(frame.len(), 1 + 72);
        assert_eq!(frame[0], ORDER_TAG);
        assert_eq!(frame[1..9], 0x0102030405060708i64.to_le_bytes());
        assert_eq!(frame[9..17], 0x1112131415161718i64.to_le_bytes());
        assert_eq!(frame[17..25], 0x2122232425262728i64.to_le_bytes());
        assert_eq!(frame[25..33], 0x3132333435363738i64.to_le_bytes());
        assert_eq!(frame[33..41], 0x4142434445464748i64.to_le_bytes());
        assert_eq!(frame[41..57], [0; 16]); // prev/next hooks
        assert_eq!(frame[57..61], 0x51525354u32.to_le_bytes());
        assert_eq!(frame[61], 1); // side::sell
        assert_eq!(frame[62], 0); // order_type::limit
        assert_eq!(frame[63], 2); // order_action::modify
        assert_eq!(frame[64..73], [0; 9]); // padding + recv_ns
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
