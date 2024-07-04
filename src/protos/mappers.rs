use protobuf::{EnumOrUnknown, MessageField, SpecialFields};

use crate::index_mapping::{IndexMapping, IndexMappingLayout};
use crate::proto::ddsketch::index_mapping::Interpolation;
use crate::store::{Store, UnboundedSizeDenseStore};
use crate::{proto, DDSketch};

impl From<MessageField<proto::ddsketch::IndexMapping>> for IndexMapping {
    fn from(proto: MessageField<proto::ddsketch::IndexMapping>) -> Self {
        let gamma = proto.gamma;
        let index_offset = proto.indexOffset;

        match proto.interpolation.unwrap() {
            Interpolation::NONE => {
                IndexMapping::with_gamma_offset(IndexMappingLayout::LOG, gamma, index_offset)
                    .unwrap()
            }
            Interpolation::CUBIC => {
                IndexMapping::with_gamma_offset(IndexMappingLayout::LogCubic, gamma, index_offset)
                    .unwrap()
            }
            _ => {
                panic!("Unsupported interpolation type!");
            }
        }
    }
}

impl From<IndexMapping> for MessageField<proto::ddsketch::IndexMapping> {
    fn from(value: IndexMapping) -> Self {
        match value {
            IndexMapping::LogarithmicMapping(
                gamma,
                index_offset,
                _multiplier,
                _relative_accuracy,
            ) => MessageField::from(Some(proto::ddsketch::IndexMapping {
                gamma,
                indexOffset: index_offset,
                interpolation: EnumOrUnknown::from(Interpolation::NONE),
                special_fields: SpecialFields::default(),
            })),
            IndexMapping::CubicallyInterpolatedMapping(
                gamma,
                index_offset,
                _multiplier,
                _relative_accuracy,
            ) => MessageField::from(Some(proto::ddsketch::IndexMapping {
                gamma,
                indexOffset: index_offset,
                interpolation: EnumOrUnknown::from(Interpolation::CUBIC),
                special_fields: SpecialFields::default(),
            })),
        }
    }
}

impl From<MessageField<proto::ddsketch::Store>> for UnboundedSizeDenseStore {
    fn from(proto: MessageField<proto::ddsketch::Store>) -> Self {
        let mut store = UnboundedSizeDenseStore::new();
        // add all from bin count map
        for (index, count) in proto.binCounts.iter() {
            store.add(*index, *count);
        }
        // add other indices
        let mut index = proto.contiguousBinIndexOffset;

        for count in proto.contiguousBinCounts.iter() {
            store.add(index, *count);
            index += 1;
        }

        store
    }
}

impl From<Box<dyn Store>> for MessageField<proto::ddsketch::Store> {
    fn from(value: Box<dyn Store>) -> Self {
        let mut proto_store = proto::ddsketch::Store::new();

        if !value.is_empty() {
            proto_store.contiguousBinIndexOffset = value.get_min_index();
            let mut i = value.get_min_index() - value.get_offset();
            let limit = value.get_max_index() - value.get_offset();

            while i <= limit {
                proto_store.contiguousBinCounts.push(value.get_count(i));
                i += 1;
            }
        }

        MessageField::from(Some(proto_store))
    }
}

impl From<proto::ddsketch::DDSketch> for DDSketch {
    fn from(proto: proto::ddsketch::DDSketch) -> Self {
        let index_mapping: IndexMapping = proto.mapping.into();
        let negative_value_store: UnboundedSizeDenseStore = proto.negativeValues.into();
        let positive_value_store: UnboundedSizeDenseStore = proto.positiveValues.into();

        DDSketch {
            index_mapping: index_mapping.clone(),
            min_indexed_value: f64::max(0.0, index_mapping.min_indexable_value()),
            max_indexed_value: index_mapping.max_indexable_value(),
            negative_value_store: Box::from(negative_value_store),
            positive_value_store: Box::from(positive_value_store),
            zero_count: proto.zeroCount,
        }
    }
}

impl From<DDSketch> for proto::ddsketch::DDSketch {
    fn from(value: DDSketch) -> Self {
        let positive_values: MessageField<proto::ddsketch::Store> =
            value.positive_value_store.into();
        let negative_values: MessageField<proto::ddsketch::Store> =
            value.negative_value_store.into();
        let mapping: MessageField<proto::ddsketch::IndexMapping> = value.index_mapping.into();

        proto::ddsketch::DDSketch {
            mapping,
            positiveValues: positive_values,
            negativeValues: negative_values,
            zeroCount: value.zero_count,
            special_fields: SpecialFields::default(),
        }
    }
}
