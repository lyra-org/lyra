// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! MD5 as RFC 1321 defines it.

const INITIAL_STATE: [u32; 4] = [0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476];

const SHIFTS: [[u32; 4]; 4] = [
    [7, 12, 17, 22],
    [5, 9, 14, 20],
    [4, 11, 16, 23],
    [6, 10, 15, 21],
];

/// `floor(2^32 * abs(sin(i + 1)))` for each of the 64 steps.
#[rustfmt::skip]
const SINES: [u32; 64] = [
    0xd76a_a478, 0xe8c7_b756, 0x2420_70db, 0xc1bd_ceee, 0xf57c_0faf, 0x4787_c62a, 0xa830_4613, 0xfd46_9501,
    0x6980_98d8, 0x8b44_f7af, 0xffff_5bb1, 0x895c_d7be, 0x6b90_1122, 0xfd98_7193, 0xa679_438e, 0x49b4_0821,
    0xf61e_2562, 0xc040_b340, 0x265e_5a51, 0xe9b6_c7aa, 0xd62f_105d, 0x0244_1453, 0xd8a1_e681, 0xe7d3_fbc8,
    0x21e1_cde6, 0xc337_07d6, 0xf4d5_0d87, 0x455a_14ed, 0xa9e3_e905, 0xfcef_a3f8, 0x676f_02d9, 0x8d2a_4c8a,
    0xfffa_3942, 0x8771_f681, 0x6d9d_6122, 0xfde5_380c, 0xa4be_ea44, 0x4bde_cfa9, 0xf6bb_4b60, 0xbebf_bc70,
    0x289b_7ec6, 0xeaa1_27fa, 0xd4ef_3085, 0x0488_1d05, 0xd9d4_d039, 0xe6db_99e5, 0x1fa2_7cf8, 0xc4ac_5665,
    0xf429_2244, 0x432a_ff97, 0xab94_23a7, 0xfc93_a039, 0x655b_59c3, 0x8f0c_cc92, 0xffef_f47d, 0x8584_5dd1,
    0x6fa8_7e4f, 0xfe2c_e6e0, 0xa301_4314, 0x4e08_11a1, 0xf753_7e82, 0xbd3a_f235, 0x2ad7_d2bb, 0xeb86_d391,
];

const BLOCK_LEN: usize = 64;
const LENGTH_LEN: usize = 8;

pub(crate) fn digest(input: &[u8]) -> [u8; 16] {
    let mut state = INITIAL_STATE;
    let mut blocks = input.chunks_exact(BLOCK_LEN);
    for block in &mut blocks {
        compress(&mut state, block);
    }

    let rest = blocks.remainder();
    let mut padding = [0u8; 2 * BLOCK_LEN];
    padding[..rest.len()].copy_from_slice(rest);
    padding[rest.len()] = 0x80;
    let padded_len = if rest.len() < BLOCK_LEN - LENGTH_LEN {
        BLOCK_LEN
    } else {
        2 * BLOCK_LEN
    };
    let bit_len = (input.len() as u64).wrapping_mul(8);
    padding[padded_len - LENGTH_LEN..padded_len].copy_from_slice(&bit_len.to_le_bytes());
    for block in padding[..padded_len].chunks_exact(BLOCK_LEN) {
        compress(&mut state, block);
    }

    let mut output = [0u8; 16];
    for (bytes, word) in output.chunks_exact_mut(4).zip(state) {
        bytes.copy_from_slice(&word.to_le_bytes());
    }
    output
}

fn compress(state: &mut [u32; 4], block: &[u8]) {
    let mut words = [0u32; 16];
    for (word, bytes) in words.iter_mut().zip(block.chunks_exact(4)) {
        *word = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    }

    let [mut a, mut b, mut c, mut d] = *state;
    for step in 0..64 {
        let (mixed, word) = match step / 16 {
            0 => ((b & c) | (!b & d), step),
            1 => ((d & b) | (!d & c), (5 * step + 1) % 16),
            2 => (b ^ c ^ d, (3 * step + 5) % 16),
            _ => (c ^ (b | !d), (7 * step) % 16),
        };
        let rotated = a
            .wrapping_add(mixed)
            .wrapping_add(SINES[step])
            .wrapping_add(words[word])
            .rotate_left(SHIFTS[step / 16][step % 4]);
        (a, b, c, d) = (d, b.wrapping_add(rotated), b, c);
    }

    for (word, added) in state.iter_mut().zip([a, b, c, d]) {
        *word = word.wrapping_add(added);
    }
}

#[cfg(test)]
mod tests {
    use super::digest;

    fn hex(input: &[u8]) -> String {
        digest(input)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    #[test]
    fn matches_the_rfc_1321_test_suite() {
        let cases: [(&str, &str); 7] = [
            ("", "d41d8cd98f00b204e9800998ecf8427e"),
            ("a", "0cc175b9c0f1b6a831c399e269772661"),
            ("abc", "900150983cd24fb0d6963f7d28e17f72"),
            ("message digest", "f96b697d7cb7938d525a2f31aaf161d0"),
            (
                "abcdefghijklmnopqrstuvwxyz",
                "c3fcd3d76192e4007dfb496cca67e13b",
            ),
            (
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
                "d174ab98d277d9f5a5611c2c9f419d9f",
            ),
            (
                "12345678901234567890123456789012345678901234567890123456789012345678901234567890",
                "57edf4a22be3c955ac49da2e2107b67a",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(hex(input.as_bytes()), expected, "input {input:?}");
        }
    }

    #[test]
    fn pads_inputs_around_the_block_boundary() {
        let cases = [
            (55, "ef1772b6dff9a122358552954ad0df65"),
            (56, "3b0c8ac703f828b04c6c197006d17218"),
            (64, "014842d480b571495a4a0363793f7367"),
            (119, "8a7bd0732ed6a28ce75f6dabc90e1613"),
        ];
        for (len, expected) in cases {
            assert_eq!(hex(&vec![b'a'; len]), expected, "{len} bytes");
        }
    }
}
