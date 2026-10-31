// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

/// A set of slots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Bits {
    words: Vec<u64>,
}

impl Bits {
    pub(crate) fn empty(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(64)],
        }
    }

    pub(crate) fn full(len: usize) -> Self {
        let mut words = vec![u64::MAX; len.div_ceil(64)];
        if let Some(last) = words.last_mut()
            && !len.is_multiple_of(64)
        {
            *last = (1 << (len % 64)) - 1;
        }
        Self { words }
    }

    pub(crate) fn of(len: usize, slots: impl IntoIterator<Item = u32>) -> Self {
        let mut bits = Self::empty(len);
        for slot in slots {
            bits.insert(slot);
        }
        bits
    }

    pub(crate) fn insert(&mut self, slot: u32) {
        self.words[slot as usize / 64] |= 1 << (slot % 64);
    }

    pub(crate) fn contains(&self, slot: u32) -> bool {
        self.words
            .get(slot as usize / 64)
            .is_some_and(|word| word & (1 << (slot % 64)) != 0)
    }

    pub(crate) fn and(&mut self, other: &Self) {
        for (word, other) in self.words.iter_mut().zip(&other.words) {
            *word &= other;
        }
    }

    pub(crate) fn and_not(&mut self, other: &Self) {
        for (word, other) in self.words.iter_mut().zip(&other.words) {
            *word &= !other;
        }
    }

    pub(crate) fn or(&mut self, other: &Self) {
        for (word, other) in self.words.iter_mut().zip(&other.words) {
            *word |= other;
        }
    }

    pub(crate) fn count(&self) -> usize {
        self.words
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.words.iter().all(|word| *word == 0)
    }

    pub(crate) fn memory(&self) -> usize {
        self.words.capacity() * 8
    }

    /// The slots in ascending order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.words.iter().enumerate().flat_map(|(index, word)| {
            let mut word = *word;
            std::iter::from_fn(move || {
                (word != 0).then(|| {
                    let bit = word.trailing_zeros();
                    word &= word - 1;
                    index as u32 * 64 + bit
                })
            })
        })
    }
}

/// Edges from each slot of one table to slots of another, each target once per source.
#[derive(Debug, Default)]
pub(crate) struct Links {
    offsets: Vec<u32>,
    targets: Vec<u32>,
}

impl Links {
    /// Links from `sources` slots, given as `(source, target)` pairs in any order.
    pub(crate) fn new(sources: usize, mut pairs: Vec<(u32, u32)>) -> Self {
        pairs.sort_unstable();
        pairs.dedup();
        let mut offsets = Vec::with_capacity(sources + 1);
        offsets.push(0);
        let mut pairs = pairs.into_iter().peekable();
        let mut targets = Vec::with_capacity(pairs.len());
        for source in 0..sources as u32 {
            while let Some((_, target)) = pairs.next_if(|(from, _)| *from == source) {
                targets.push(target);
            }
            offsets.push(targets.len() as u32);
        }
        Self { offsets, targets }
    }

    /// The same links followed the other way, from the `len` slots of the target table.
    pub(crate) fn inverted(&self, len: usize) -> Self {
        Self::new(
            len,
            self.pairs()
                .map(|(from, to)| (to, from))
                .collect::<Vec<_>>(),
        )
    }

    fn pairs(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        (0..self.offsets.len().saturating_sub(1) as u32)
            .flat_map(move |source| self.of(source).iter().map(move |target| (source, *target)))
    }

    pub(crate) fn of(&self, source: u32) -> &[u32] {
        match (
            self.offsets.get(source as usize),
            self.offsets.get(source as usize + 1),
        ) {
            (Some(start), Some(end)) => &self.targets[*start as usize..*end as usize],
            _ => &[],
        }
    }

    /// The targets of every slot in `sources`, as a set over `len` target slots.
    pub(crate) fn reach(&self, sources: &Bits, len: usize) -> Bits {
        self.reach_slots(sources.iter(), len)
    }

    pub(crate) fn reach_slots(&self, sources: impl IntoIterator<Item = u32>, len: usize) -> Bits {
        let mut reached = Bits::empty(len);
        for source in sources {
            for target in self.of(source) {
                reached.insert(*target);
            }
        }
        reached
    }

    /// The slots with at least one link, as a set over `len` slots.
    pub(crate) fn linked(&self, len: usize) -> Bits {
        Bits::of(
            len,
            (0..len as u32).filter(|source| !self.of(*source).is_empty()),
        )
    }

    pub(crate) fn memory(&self) -> usize {
        (self.offsets.capacity() + self.targets.capacity()) * 4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bits_combine_and_list_their_slots() {
        let mut bits = Bits::of(130, [0, 5, 64, 129]);
        assert_eq!(bits.iter().collect::<Vec<_>>(), vec![0, 5, 64, 129]);
        assert_eq!(Bits::full(130).count(), 130);
        bits.and(&Bits::of(130, [5, 129, 7]));
        assert_eq!(bits.iter().collect::<Vec<_>>(), vec![5, 129]);
        bits.or(&Bits::of(130, [1]));
        bits.and_not(&Bits::of(130, [129]));
        assert_eq!(bits.iter().collect::<Vec<_>>(), vec![1, 5]);
        assert!(!bits.contains(200));
    }

    #[test]
    fn links_dedup_and_invert() {
        let links = Links::new(3, vec![(2, 0), (0, 1), (0, 1), (0, 0)]);
        assert_eq!(links.of(0), &[0, 1]);
        assert_eq!(links.of(1), &[] as &[u32]);
        assert_eq!(links.of(2), &[0]);
        let inverted = links.inverted(2);
        assert_eq!(inverted.of(0), &[0, 2]);
        assert_eq!(inverted.of(1), &[0]);
        assert_eq!(
            links.reach(&Bits::of(3, [2]), 2).iter().collect::<Vec<_>>(),
            vec![0]
        );
    }
}
