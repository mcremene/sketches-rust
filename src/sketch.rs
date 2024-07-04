use crate::error::Error;
use crate::index_mapping::IndexMappingLayout::{LogCubic, LOG};
use crate::index_mapping::{IndexMapping, IndexMappingLayout};
use crate::input::Input;
use crate::output::Output;
use crate::serde;
use crate::store::{
    BinEncodingMode, CollapsingHighestDenseStore, CollapsingLowestDenseStore, Store,
    UnboundedSizeDenseStore,
};

pub struct DDSketch {
    pub index_mapping: IndexMapping,
    pub min_indexed_value: f64,
    pub max_indexed_value: f64,
    pub negative_value_store: Box<dyn Store>,
    pub positive_value_store: Box<dyn Store>,
    pub zero_count: f64,
}

#[derive(PartialEq)]
pub struct Flag {
    marker: u8,
}

pub enum FlagType {
    SketchFeatures = 0b00,
    PositiveStore = 0b01,
    IndexMapping = 0b10,
    NegativeStore = 0b11,
}

impl DDSketch {
    pub fn accept(&mut self, value: f64) {
        self.accept_with_count(value, 1.0);
    }

    pub fn accept_with_count(&mut self, value: f64, count: f64) {
        if count < 0.0 {
            return;
        }

        if value < -self.max_indexed_value || value > self.max_indexed_value {
            return;
        }

        if value > self.min_indexed_value {
            self.positive_value_store
                .add(self.index_mapping.index(value), 1.0);
        } else if value < -self.min_indexed_value {
            self.negative_value_store
                .add(self.index_mapping.index(-value), 1.0);
        } else {
            self.zero_count += 1.0;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.zero_count == 0.0
            && self.negative_value_store.is_empty()
            && self.positive_value_store.is_empty()
    }

    pub fn clear(&mut self) {
        self.negative_value_store.clear();
        self.positive_value_store.clear();
        self.zero_count = 0.0;
    }

    pub fn get_count(&mut self) -> f64 {
        self.zero_count
            + self.negative_value_store.get_total_count()
            + self.positive_value_store.get_total_count()
    }

    pub fn get_sum(&mut self) -> Option<f64> {
        let count = self.get_count();
        if count <= 0.0 {
            return None;
        }

        let mut sum = 0.0;
        sum -= self.negative_value_store.get_sum(&self.index_mapping);
        sum += self.positive_value_store.get_sum(&self.index_mapping);

        Some(sum)
    }

    pub fn get_max(&mut self) -> Option<f64> {
        if !self.positive_value_store.is_empty() {
            Some(
                self.index_mapping
                    .value(self.positive_value_store.get_max_index()),
            )
        } else if self.zero_count > 0.0 {
            Some(0.0)
        } else if !self.negative_value_store.is_empty() {
            Some(
                -self
                    .index_mapping
                    .value(self.negative_value_store.get_min_index()),
            )
        } else {
            None
        }
    }

    pub fn get_min(&mut self) -> Option<f64> {
        if !self.negative_value_store.is_empty() {
            Some(
                -self
                    .index_mapping
                    .value(self.negative_value_store.get_max_index()),
            )
        } else if self.zero_count > 0.0 {
            Some(0.0)
        } else if !self.positive_value_store.is_empty() {
            Some(
                self.index_mapping
                    .value(self.positive_value_store.get_min_index()),
            )
        } else {
            None
        }
    }

    pub fn get_average(&mut self) -> Option<f64> {
        let count = self.get_count();
        if count <= 0.0 {
            return None;
        }
        Some(self.get_sum()? / count)
    }

    pub fn get_value_at_quantile(self: &mut DDSketch, quantile: f64) -> Option<f64> {
        if !(0.0..=1.0).contains(&quantile) {
            return None;
        }

        let count = self.get_count();
        if count <= 0.0 {
            return None;
        }

        let rank = quantile * (count - 1.0);

        let mut n: f64 = 0.0;

        let negative_bin_iterator = self.negative_value_store.get_descending_iter();
        for bin in negative_bin_iterator {
            n += bin.1;
            if n > rank {
                return Some(-self.index_mapping.value(bin.0));
            }
        }

        n += self.zero_count;
        if n > rank {
            return Some(0.0);
        }

        let positive_bin_iterator = self.positive_value_store.get_ascending_iter();
        for bin in positive_bin_iterator {
            n += bin.1;
            if n > rank {
                return Some(self.index_mapping.value(bin.0));
            }
        }

        None
    }

    pub fn decode_and_merge_with(&mut self, bytes: &Vec<u8>) -> Result<(), Error> {
        let mut input = Input::wrap(bytes);
        while input.has_remaining() {
            let flag = Flag::decode(&mut input)?;
            let flag_type = flag.get_type()?;
            match flag_type {
                FlagType::PositiveStore => {
                    let mode = BinEncodingMode::of_flag(flag.get_marker())?;
                    self.positive_value_store
                        .decode_and_merge_with(&mut input, mode)?;
                }
                FlagType::NegativeStore => {
                    let mode = BinEncodingMode::of_flag(flag.get_marker())?;
                    self.negative_value_store
                        .decode_and_merge_with(&mut input, mode)?;
                }
                FlagType::IndexMapping => {
                    let layout = IndexMappingLayout::of_flag(&flag)?;
                    let gamma = input.read_double_le()?;
                    let index_offset = input.read_double_le()?;
                    let decoded_index_mapping =
                        IndexMapping::with_gamma_offset(layout, gamma, index_offset)?;
                    if self.index_mapping != decoded_index_mapping {
                        return Err(Error::InvalidArgument("Unmatched IndexMapping"));
                    }
                }
                FlagType::SketchFeatures => {
                    if Flag::ZERO_COUNT == flag {
                        self.zero_count += serde::decode_var_double(&mut input)?;
                    } else {
                        serde::ignore_exact_summary_statistic_flags(&mut input, flag)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn merge_with(&mut self, other: &DDSketch) -> Result<(), Error> {
        if self.index_mapping != other.index_mapping {
            return Err(Error::InvalidArgument("Unmatched indexMapping."));
        }
        self.negative_value_store
            .merge_with(other.negative_value_store.get_descending_stream());
        self.positive_value_store
            .merge_with(other.positive_value_store.get_descending_stream());
        self.zero_count += other.zero_count;
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut output = Output::with_capacity(64);
        self.index_mapping.encode(&mut output)?;

        if self.zero_count != 0.0 {
            Flag::ZERO_COUNT.encode(&mut output)?;
            serde::encode_var_double(&mut output, self.zero_count)?;
        }

        self.positive_value_store
            .encode(&mut output, FlagType::PositiveStore)?;
        self.negative_value_store
            .encode(&mut output, FlagType::NegativeStore)?;

        Ok(output.trim())
    }

    pub fn decode(bytes: &Vec<u8>) -> Result<DDSketch, Error> {
        let mut input = Input::wrap(bytes);
        let mut positive_value_store = UnboundedSizeDenseStore::new();
        let mut negative_value_store = UnboundedSizeDenseStore::new();
        let mut index_mapping = None;
        let mut zero_count = 0.0;
        while input.has_remaining() {
            let flag = Flag::decode(&mut input)?;
            let flag_type = flag.get_type()?;
            match flag_type {
                FlagType::PositiveStore => {
                    let mode = BinEncodingMode::of_flag(flag.get_marker())?;
                    positive_value_store.decode_and_merge_with(&mut input, mode)?;
                }
                FlagType::NegativeStore => {
                    let mode = BinEncodingMode::of_flag(flag.get_marker())?;
                    negative_value_store.decode_and_merge_with(&mut input, mode)?;
                }
                FlagType::IndexMapping => {
                    let layout = IndexMappingLayout::of_flag(&flag)?;
                    let gamma = input.read_double_le()?;
                    let index_offset = input.read_double_le()?;
                    index_mapping = Some(IndexMapping::with_gamma_offset(
                        layout,
                        gamma,
                        index_offset,
                    )?);
                }
                FlagType::SketchFeatures => {
                    if Flag::ZERO_COUNT == flag {
                        zero_count += serde::decode_var_double(&mut input)?;
                    } else {
                        serde::ignore_exact_summary_statistic_flags(&mut input, flag)?;
                    }
                }
            }
        }

        match index_mapping {
            Some(mapping) => {
                let min_indexed_value = f64::max(0.0, mapping.min_indexable_value());
                let max_indexed_value = mapping.max_indexable_value();
                Ok(DDSketch {
                    index_mapping: mapping,
                    negative_value_store: Box::new(negative_value_store),
                    positive_value_store: Box::new(positive_value_store),
                    min_indexed_value,
                    max_indexed_value,
                    zero_count,
                })
            }
            None => Err(Error::InvalidArgument("No IndexMapping decoded")),
        }
    }
}

// factory methods
impl DDSketch {
    pub fn collapsing_lowest_dense(
        relative_accuracy: f64,
        max_num_bins: usize,
    ) -> Result<DDSketch, Error> {
        let index_mapping = IndexMapping::with_relative_accuracy(LogCubic, relative_accuracy)?;
        let negative_value_store = CollapsingLowestDenseStore::with_capacity(max_num_bins)?;
        let positive_value_store = CollapsingLowestDenseStore::with_capacity(max_num_bins)?;
        let min_indexed_value = f64::max(0.0, index_mapping.min_indexable_value());
        let max_indexed_value = index_mapping.max_indexable_value();
        let zero_count = 0.0;

        Ok(DDSketch {
            index_mapping,
            negative_value_store: Box::new(negative_value_store),
            positive_value_store: Box::new(positive_value_store),
            min_indexed_value,
            max_indexed_value,
            zero_count,
        })
    }

    pub fn collapsing_highest_dense(
        relative_accuracy: f64,
        max_num_bins: usize,
    ) -> Result<DDSketch, Error> {
        let index_mapping = IndexMapping::with_relative_accuracy(LogCubic, relative_accuracy)?;
        let negative_value_store = CollapsingHighestDenseStore::with_capacity(max_num_bins)?;
        let positive_value_store = CollapsingHighestDenseStore::with_capacity(max_num_bins)?;
        let min_indexed_value = f64::max(0.0, index_mapping.min_indexable_value());
        let max_indexed_value = index_mapping.max_indexable_value();
        let zero_count = 0.0;
        Ok(DDSketch {
            index_mapping,
            negative_value_store: Box::new(negative_value_store),
            positive_value_store: Box::new(positive_value_store),
            min_indexed_value,
            max_indexed_value,
            zero_count,
        })
    }

    pub fn unbounded_dense(relative_accuracy: f64) -> Result<DDSketch, Error> {
        let index_mapping = IndexMapping::with_relative_accuracy(LogCubic, relative_accuracy)?;
        let negative_value_store = UnboundedSizeDenseStore::new();
        let positive_value_store = UnboundedSizeDenseStore::new();
        let min_indexed_value = f64::max(0.0, index_mapping.min_indexable_value());
        let max_indexed_value = index_mapping.max_indexable_value();
        let zero_count = 0.0;
        Ok(DDSketch {
            index_mapping,
            negative_value_store: Box::new(negative_value_store),
            positive_value_store: Box::new(positive_value_store),
            min_indexed_value,
            max_indexed_value,
            zero_count,
        })
    }

    pub fn logarithmic_collapsing_lowest_dense(
        relative_accuracy: f64,
        max_num_bins: usize,
    ) -> Result<DDSketch, Error> {
        let index_mapping = IndexMapping::with_relative_accuracy(LOG, relative_accuracy)?;
        let negative_value_store = CollapsingLowestDenseStore::with_capacity(max_num_bins)?;
        let positive_value_store = CollapsingLowestDenseStore::with_capacity(max_num_bins)?;
        let min_indexed_value = f64::max(0.0, index_mapping.min_indexable_value());
        let max_indexed_value = index_mapping.max_indexable_value();
        let zero_count = 0.0;
        Ok(DDSketch {
            index_mapping,
            negative_value_store: Box::new(negative_value_store),
            positive_value_store: Box::new(positive_value_store),
            min_indexed_value,
            max_indexed_value,
            zero_count,
        })
    }

    pub fn logarithmic_collapsing_highest_dense(
        relative_accuracy: f64,
        max_num_bins: usize,
    ) -> Result<DDSketch, Error> {
        let index_mapping = IndexMapping::with_relative_accuracy(LOG, relative_accuracy)?;
        let negative_value_store = CollapsingHighestDenseStore::with_capacity(max_num_bins)?;
        let positive_value_store = CollapsingHighestDenseStore::with_capacity(max_num_bins)?;
        let min_indexed_value = f64::max(0.0, index_mapping.min_indexable_value());
        let max_indexed_value = index_mapping.max_indexable_value();
        let zero_count = 0.0;
        Ok(DDSketch {
            index_mapping,
            negative_value_store: Box::new(negative_value_store),
            positive_value_store: Box::new(positive_value_store),
            min_indexed_value,
            max_indexed_value,
            zero_count,
        })
    }

    pub fn logarithmic_unbounded_size_dense_store(
        relative_accuracy: f64,
    ) -> Result<DDSketch, Error> {
        let index_mapping = IndexMapping::with_relative_accuracy(LOG, relative_accuracy)?;
        let negative_value_store = UnboundedSizeDenseStore::new();
        let positive_value_store = UnboundedSizeDenseStore::new();
        let min_indexed_value = f64::max(0.0, index_mapping.min_indexable_value());
        let max_indexed_value = index_mapping.max_indexable_value();
        let zero_count = 0.0;
        Ok(DDSketch {
            index_mapping,
            negative_value_store: Box::new(negative_value_store),
            positive_value_store: Box::new(positive_value_store),
            min_indexed_value,
            max_indexed_value,
            zero_count,
        })
    }
}

impl Flag {
    pub const ZERO_COUNT: Flag = Flag::with_type(FlagType::SketchFeatures, 1);
    pub const COUNT: Flag = Flag::with_type(FlagType::SketchFeatures, 0x28);
    pub const SUM: Flag = Flag::with_type(FlagType::SketchFeatures, 0x21);
    pub const MIN: Flag = Flag::with_type(FlagType::SketchFeatures, 0x22);
    pub const MAX: Flag = Flag::with_type(FlagType::SketchFeatures, 0x23);

    pub const fn new(marker: u8) -> Flag {
        Flag { marker }
    }

    pub fn decode(input: &mut Input) -> Result<Flag, Error> {
        let marker = input.read_byte()?;
        Ok(Flag::new(marker))
    }

    pub fn get_type(&self) -> Result<FlagType, Error> {
        FlagType::value_of(self.marker & 3)
    }

    pub fn get_marker(&self) -> u8 {
        self.marker
    }

    pub fn encode(&self, output: &mut Output) -> Result<(), Error> {
        output.write_byte(self.marker)
    }

    pub const fn with_type(flag_type: FlagType, sub_flag: u8) -> Flag {
        let t = flag_type as u8;
        Flag::new(t | (sub_flag << 2))
    }
}

impl FlagType {
    pub fn value_of(t: u8) -> Result<FlagType, Error> {
        match t {
            0b00 => Ok(FlagType::SketchFeatures),
            0b01 => Ok(FlagType::PositiveStore),
            0b10 => Ok(FlagType::IndexMapping),
            0b11 => Ok(FlagType::NegativeStore),
            _ => Err(Error::InvalidArgument("Unknown FlagType.")),
        }
    }
}
