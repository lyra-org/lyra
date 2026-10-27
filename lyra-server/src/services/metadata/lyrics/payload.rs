// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use serde::{
    Deserialize,
    Serialize,
};

use lyra_db::lyrics::{
    LineInput,
    LyricsInput,
    WordInput,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PluginLyricWordInput {
    pub(crate) ts_ms: u64,
    pub(crate) char_start: u32,
    pub(crate) char_end: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PluginLyricLineInput {
    pub(crate) ts_ms: u64,
    pub(crate) text: String,
    #[serde(default)]
    pub(crate) words: Vec<PluginLyricWordInput>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PluginLyricsInput {
    pub(crate) id: String,
    pub(crate) language: String,
    #[serde(default)]
    pub(crate) plain_text: String,
    #[serde(default)]
    pub(crate) lines: Vec<PluginLyricLineInput>,
}

impl PluginLyricsInput {
    pub(crate) fn into_lyrics_input(self, now_ms: u64) -> anyhow::Result<(String, LyricsInput)> {
        anyhow::ensure!(!self.id.trim().is_empty(), "lyrics id cannot be empty");

        Ok((
            self.id,
            LyricsInput {
                language: self.language,
                plain_text: self.plain_text,
                lines: self
                    .lines
                    .into_iter()
                    .map(|line| LineInput {
                        ts_ms: line.ts_ms,
                        text: line.text,
                        words: line
                            .words
                            .into_iter()
                            .map(|word| WordInput {
                                ts_ms: word.ts_ms,
                                char_start: word.char_start,
                                char_end: word.char_end,
                            })
                            .collect(),
                    })
                    .collect(),
                last_checked_at: now_ms,
            },
        ))
    }
}
