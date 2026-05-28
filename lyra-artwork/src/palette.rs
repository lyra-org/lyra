use std::{
    cmp::Ordering,
    collections::{
        HashMap,
        HashSet,
    },
    f32::consts::TAU,
};

use palette::{
    FromColor,
    IntoColor,
    LinSrgb,
    Oklab,
    Srgb,
};

use crate::ArtworkSource;

const SAMPLE_SIZE: u32 = 64;
const MIN_ALPHA: u8 = 32;
const MAX_CANDIDATES_PER_SOURCE: usize = 8;
const TARGET_PALETTE_SIZE: usize = 5;
const MIN_ACCENT_CHROMA: f32 = 0.04;

#[derive(Clone, Copy, Debug)]
pub(crate) struct PaletteColor {
    pub(crate) rgb: [u8; 3],
    pub(crate) lab: Oklab,
}

#[derive(Clone, Debug)]
struct Candidate {
    color: PaletteColor,
    source_index: usize,
    score: f32,
    tie_breaker: u128,
}

#[derive(Default)]
struct Bin {
    weight: f32,
    l: f32,
    a: f32,
    b: f32,
}

pub(crate) fn select_palette(sources: &[&ArtworkSource], seed: &str) -> Vec<PaletteColor> {
    let mut candidates = Vec::new();
    for (source_index, source) in sources.iter().enumerate() {
        candidates.extend(extract_candidates(source, source_index, seed));
    }

    if candidates.is_empty() {
        return Vec::new();
    }

    candidates.sort_by(compare_candidate);

    let mut selected = Vec::new();
    let first = candidates
        .iter()
        .max_by(|a, b| compare_f32(base_color_score(a), base_color_score(b)))
        .cloned()
        .unwrap_or_else(|| candidates[0].clone());
    selected.push(first);

    while selected.len() < TARGET_PALETTE_SIZE && selected.len() < candidates.len() {
        let Some(next) = candidates
            .iter()
            .filter(|candidate| !selected.iter().any(|chosen| color_close(candidate, chosen)))
            .max_by(|a, b| {
                compare_f32(
                    combination_score(a, &selected),
                    combination_score(b, &selected),
                )
                .then_with(|| b.tie_breaker.cmp(&a.tie_breaker))
            })
            .cloned()
        else {
            break;
        };
        selected.push(next);
    }

    let mut colors = render_palette(&selected, &candidates, seed);
    synthesize_missing_colors(&mut colors, seed);
    colors
}

fn render_palette(
    selected: &[Candidate],
    candidates: &[Candidate],
    seed: &str,
) -> Vec<PaletteColor> {
    let base_source = selected
        .iter()
        .filter(|candidate| chroma(candidate.color.lab) >= 0.025)
        .max_by(|a, b| compare_f32(base_color_score(a), base_color_score(b)))
        .or_else(|| selected.first())
        .expect("selected is non-empty");

    let mut colors = vec![shadow_color(base_source.color)];
    push_distinct_color(&mut colors, base_source.color);

    let mut accents = candidates
        .iter()
        .filter(|candidate| chroma(candidate.color.lab) >= MIN_ACCENT_CHROMA)
        .filter(|candidate| (0.22..=0.88).contains(&candidate.color.lab.l))
        .collect::<Vec<_>>();
    accents.sort_by(|a, b| {
        compare_f32(accent_score(b), accent_score(a))
            .then_with(|| a.tie_breaker.cmp(&b.tie_breaker))
    });

    for candidate in accents {
        if colors.len() >= TARGET_PALETTE_SIZE {
            break;
        }
        push_distinct_color(&mut colors, candidate.color);
    }

    if colors.len() < TARGET_PALETTE_SIZE {
        for candidate in selected {
            if colors.len() >= TARGET_PALETTE_SIZE {
                break;
            }
            push_distinct_color(&mut colors, candidate.color);
        }
    }

    if colors.len() < 4 {
        push_distinct_color(&mut colors, highlight_color(base_source.color, seed));
    }

    colors
}

fn push_distinct_color(colors: &mut Vec<PaletteColor>, color: PaletteColor) {
    if colors
        .iter()
        .all(|existing| lab_distance(existing.lab, color.lab) >= 0.055)
    {
        colors.push(color);
    }
}

fn shadow_color(color: PaletteColor) -> PaletteColor {
    let mut lab = color.lab;
    lab.l = (lab.l * 0.46).clamp(0.07, 0.28);
    lab.a *= 0.82;
    lab.b *= 0.82;
    PaletteColor {
        rgb: lab_to_rgb(lab),
        lab,
    }
}

fn highlight_color(color: PaletteColor, seed: &str) -> PaletteColor {
    let amount = deterministic_unit(seed, 97);
    let mut lab = color.lab;
    lab.l = (lab.l + 0.24 + amount * 0.08).clamp(0.56, 0.9);
    lab.a *= 1.0 + amount * 0.18;
    lab.b *= 1.0 + amount * 0.18;
    PaletteColor {
        rgb: lab_to_rgb(lab),
        lab,
    }
}

fn extract_candidates(source: &ArtworkSource, source_index: usize, seed: &str) -> Vec<Candidate> {
    let image = source.image.thumbnail(SAMPLE_SIZE, SAMPLE_SIZE).to_rgba8();
    let mut bins: HashMap<(u8, u8, u8), Bin> = HashMap::new();

    for pixel in image.pixels() {
        let [r, g, b, a] = pixel.0;
        if a < MIN_ALPHA {
            continue;
        }

        let lab = rgb_to_lab([r, g, b]);
        let chroma = chroma(lab);
        let lightness = lab.l.clamp(0.0, 1.0);
        let alpha_weight = f32::from(a) / 255.0;
        let edge_penalty = if !(0.05..=0.96).contains(&lightness) {
            0.25
        } else {
            1.0
        };
        let neutral_penalty = if chroma < 0.015 { 0.45 } else { 1.0 };
        let weight = alpha_weight * edge_penalty * neutral_penalty * (0.75 + chroma * 3.0);
        if weight <= 0.0 {
            continue;
        }

        let key = bin_key(lab);
        let bin = bins.entry(key).or_default();
        bin.weight += weight;
        bin.l += lab.l * weight;
        bin.a += lab.a * weight;
        bin.b += lab.b * weight;
    }

    let mut candidates = bins
        .into_iter()
        .filter_map(|(_, bin)| {
            if bin.weight <= 0.0 {
                return None;
            }
            let lab = Oklab::new(bin.l / bin.weight, bin.a / bin.weight, bin.b / bin.weight);
            let color = PaletteColor {
                rgb: lab_to_rgb(lab),
                lab,
            };
            let score = candidate_score(color, bin.weight);
            Some(Candidate {
                color,
                source_index,
                score,
                tie_breaker: candidate_tie_breaker(source, seed, color.rgb),
            })
        })
        .collect::<Vec<_>>();

    candidates.sort_by(compare_candidate);
    candidates.truncate(MAX_CANDIDATES_PER_SOURCE);
    candidates
}

fn bin_key(lab: Oklab) -> (u8, u8, u8) {
    let hue_bin = if chroma(lab) < 0.015 {
        36
    } else {
        (hue(lab) / 10.0).floor().clamp(0.0, 35.0) as u8
    };
    let lightness_bin = (lab.l.clamp(0.0, 0.999) * 8.0).floor() as u8;
    let chroma_bin = (chroma(lab).clamp(0.0, 0.249) * 24.0).floor() as u8;
    (hue_bin, lightness_bin, chroma_bin)
}

fn candidate_score(color: PaletteColor, weight: f32) -> f32 {
    let c = chroma(color.lab);
    let l = color.lab.l;
    let lightness_quality = 1.0 - ((l - 0.55).abs() / 0.55).min(1.0) * 0.35;
    weight.sqrt() * lightness_quality * (0.55 + c * 4.0)
}

fn base_color_score(candidate: &Candidate) -> f32 {
    let l = candidate.color.lab.l;
    let base_lightness = 1.0 - ((l - 0.34).abs() / 0.34).min(1.0);
    candidate.score * (0.75 + base_lightness * 0.5)
}

fn accent_score(candidate: &Candidate) -> f32 {
    let c = chroma(candidate.color.lab);
    let l = candidate.color.lab.l;
    let lightness_quality = 1.0 - ((l - 0.58).abs() / 0.58).min(1.0) * 0.25;
    candidate.score * (1.0 + c * 2.5) * lightness_quality
}

fn combination_score(candidate: &Candidate, selected: &[Candidate]) -> f32 {
    let min_distance = selected
        .iter()
        .map(|chosen| lab_distance(candidate.color.lab, chosen.color.lab))
        .fold(f32::INFINITY, f32::min);
    let source_bonus = if selected
        .iter()
        .any(|chosen| chosen.source_index == candidate.source_index)
    {
        0.0
    } else {
        0.25
    };
    let harmony = selected
        .iter()
        .map(|chosen| harmony_score(candidate.color.lab, chosen.color.lab))
        .fold(0.0, f32::max);
    let muddy_penalty = mud_penalty(candidate, selected);

    candidate.score + min_distance.min(0.35) * 2.0 + source_bonus + harmony * 0.35 - muddy_penalty
}

fn color_close(a: &Candidate, b: &Candidate) -> bool {
    lab_distance(a.color.lab, b.color.lab) < 0.045
}

fn mud_penalty(candidate: &Candidate, selected: &[Candidate]) -> f32 {
    let low_chroma_count = selected
        .iter()
        .filter(|chosen| chroma(chosen.color.lab) < 0.035)
        .count()
        + usize::from(chroma(candidate.color.lab) < 0.035);
    if low_chroma_count >= 3 { 0.35 } else { 0.0 }
}

fn harmony_score(a: Oklab, b: Oklab) -> f32 {
    let diff = hue_distance(hue(a), hue(b));
    let targets = [28.0, 42.0, 120.0, 150.0, 180.0];
    targets
        .into_iter()
        .map(|target| 1.0 - ((diff - target).abs() / 28.0).min(1.0))
        .fold(0.0, f32::max)
}

fn synthesize_missing_colors(colors: &mut Vec<PaletteColor>, seed: &str) {
    if colors.is_empty() {
        return;
    }

    let mut seen = HashSet::new();
    colors.retain(|color| seen.insert(color.rgb));

    let base = colors[0].lab;
    let mut nonce = 0u32;
    while colors.len() < 3 {
        let amount = deterministic_unit(seed, nonce);
        let mut lab = base;
        lab.l = match colors.len() {
            1 => (base.l * 0.62).clamp(0.08, 0.42),
            _ => (base.l + 0.28 + amount * 0.08).clamp(0.52, 0.88),
        };
        lab.a *= 1.0 + amount * 0.2;
        lab.b *= 1.0 + amount * 0.2;
        let color = PaletteColor {
            rgb: lab_to_rgb(lab),
            lab,
        };
        if seen.insert(color.rgb) {
            colors.push(color);
        }
        nonce += 1;
        if nonce > 8 {
            break;
        }
    }
}

fn candidate_tie_breaker(source: &ArtworkSource, seed: &str, rgb: [u8; 3]) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(seed.as_bytes());
    hasher.update(source.stable_id.as_bytes());
    hasher.update(source.content_hash.as_bytes());
    hasher.update(&rgb);
    let hash = hasher.finalize();
    u128::from_le_bytes(
        hash.as_bytes()[0..16]
            .try_into()
            .expect("hash slice length"),
    )
}

fn deterministic_unit(seed: &str, nonce: u32) -> f32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(seed.as_bytes());
    hasher.update(&nonce.to_le_bytes());
    let hash = hasher.finalize();
    let value = u32::from_le_bytes(hash.as_bytes()[0..4].try_into().expect("hash slice length"));
    value as f32 / u32::MAX as f32
}

pub(crate) fn rgb_to_lab(rgb: [u8; 3]) -> Oklab {
    let srgb = Srgb::new(
        f32::from(rgb[0]) / 255.0,
        f32::from(rgb[1]) / 255.0,
        f32::from(rgb[2]) / 255.0,
    );
    let linear: LinSrgb = srgb.into_linear();
    Oklab::from_color(linear)
}

pub(crate) fn lab_to_rgb(lab: Oklab) -> [u8; 3] {
    let linear: LinSrgb = lab.into_color();
    let srgb: Srgb = Srgb::from_linear(linear);
    [
        float_to_u8(srgb.red),
        float_to_u8(srgb.green),
        float_to_u8(srgb.blue),
    ]
}

fn float_to_u8(value: f32) -> u8 {
    (value.clamp(0.0, 1.0) * 255.0).round() as u8
}

pub(crate) fn chroma(lab: Oklab) -> f32 {
    lab.a.hypot(lab.b)
}

fn hue(lab: Oklab) -> f32 {
    let radians = lab.b.atan2(lab.a).rem_euclid(TAU);
    radians.to_degrees()
}

fn hue_distance(a: f32, b: f32) -> f32 {
    let diff = (a - b).abs().rem_euclid(360.0);
    diff.min(360.0 - diff)
}

fn lab_distance(a: Oklab, b: Oklab) -> f32 {
    let dl = a.l - b.l;
    let da = a.a - b.a;
    let db = a.b - b.b;
    (dl * dl + da * da + db * db).sqrt()
}

fn compare_candidate(a: &Candidate, b: &Candidate) -> Ordering {
    compare_f32(b.score, a.score).then_with(|| a.tie_breaker.cmp(&b.tie_breaker))
}

fn compare_f32(a: f32, b: f32) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}
