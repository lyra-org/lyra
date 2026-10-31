// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::collections::HashMap;

use super::bits::{
    Bits,
    Links,
};
use crate::services::search::{
    self,
    Word,
};

/// Strings stored end to end, by position.
#[derive(Debug, Default)]
pub(crate) struct Strings {
    text: String,
    ends: Vec<u32>,
}

impl Strings {
    pub(crate) fn push(&mut self, value: &str) {
        self.text.push_str(value);
        self.ends.push(self.text.len() as u32);
    }

    pub(crate) fn get(&self, index: u32) -> &str {
        let index = index as usize;
        let start = match index {
            0 => 0,
            index => self.ends[index - 1] as usize,
        };
        &self.text[start..self.ends[index] as usize]
    }

    pub(crate) fn len(&self) -> usize {
        self.ends.len()
    }

    pub(crate) fn memory(&self) -> usize {
        self.text.capacity() + self.ends.capacity() * 4
    }
}

/// Which text of an entity a word came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Field {
    Name,
    SortName,
    /// Other names the entity goes by, such as an artist's merged duplicates.
    Alias,
}

const FIELDS: usize = 3;

/// The folded words of a table's texts, sorted, each with the slots whose texts hold it.
#[derive(Debug, Default)]
pub(crate) struct Words {
    words: Strings,
    slots: [Links; FIELDS],
}

/// Collects the words of a table's texts.
#[derive(Default)]
pub(crate) struct WordsBuilder {
    ids: HashMap<Box<str>, u32>,
    pairs: [Vec<(u32, u32)>; FIELDS],
}

impl WordsBuilder {
    /// Adds the words of `folded` text to `slot`'s `field`.
    pub(crate) fn add(&mut self, slot: u32, field: Field, folded: &str) {
        let Self { ids, pairs } = self;
        search::words(folded, |word| {
            let id = match ids.get(word) {
                Some(id) => *id,
                None => {
                    let id = ids.len() as u32;
                    ids.insert(Box::from(word), id);
                    id
                }
            };
            pairs[field as usize].push((id, slot));
        });
    }

    pub(crate) fn finish(self) -> Words {
        let mut sorted = self.ids.into_iter().collect::<Vec<_>>();
        sorted.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        let mut place = vec![0; sorted.len()];
        let mut words = Strings::default();
        for (index, (word, id)) in sorted.iter().enumerate() {
            place[*id as usize] = index as u32;
            words.push(word);
        }
        let slots = self.pairs.map(|pairs| {
            Links::new(
                words.len(),
                pairs
                    .into_iter()
                    .map(|(id, slot)| (place[id as usize], slot))
                    .collect(),
            )
        });
        Words { words, slots }
    }
}

impl Words {
    /// The ids of the stored words `word` matches.
    pub(crate) fn matching(&self, word: &Word) -> Vec<u32> {
        let count = self.words.len() as u32;
        if let Some(infix) = word.infix() {
            return (0..count)
                .filter(|id| self.words.get(*id).contains(infix))
                .collect();
        }
        let mut ids = Vec::new();
        for prefix in word.prefixes() {
            let start = partition(count, |id| self.words.get(id) < prefix);
            ids.extend((start..count).take_while(|id| self.words.get(*id).starts_with(prefix)));
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// The slots of `len` whose `field` holds any of the words `ids`.
    pub(crate) fn slots(&self, field: Field, ids: &[u32], len: usize) -> Bits {
        self.slots[field as usize].reach_slots(ids.iter().copied(), len)
    }

    pub(crate) fn memory(&self) -> usize {
        self.words.memory() + self.slots.iter().map(Links::memory).sum::<usize>()
    }
}

/// The first id in `0..count` for which `before` is false, for a `before` that holds for a prefix
/// of the ids.
fn partition(count: u32, before: impl Fn(u32) -> bool) -> u32 {
    let (mut low, mut high) = (0, count);
    while low < high {
        let middle = low + (high - low) / 2;
        if before(middle) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::search::{
        Needle,
        fold,
    };

    #[test]
    fn words_find_the_slots_whose_words_start_with_a_query_word() {
        let mut builder = WordsBuilder::default();
        for (slot, name) in ["AC/DC", "Accept", "Blue Öyster Cult", "夜に駆ける"]
            .into_iter()
            .enumerate()
        {
            builder.add(slot as u32, Field::Name, &fold(name));
        }
        builder.add(1, Field::Alias, "acdc");
        let words = builder.finish();
        let find = |query: &str, field| {
            let needle = Needle::parse(query).expect("a query");
            let ids = words.matching(&needle.words()[0]);
            words.slots(field, &ids, 4).iter().collect::<Vec<_>>()
        };
        assert_eq!(find("ac", Field::Name), vec![0, 1]);
        assert_eq!(find("acdc", Field::Name), vec![0]);
        assert_eq!(find("acdc", Field::Alias), vec![1]);
        assert_eq!(find("oyster", Field::Name), vec![2]);
        assert_eq!(find("駆け", Field::Name), vec![3]);
        assert_eq!(find("zz", Field::Name), Vec::<u32>::new());
    }
}
