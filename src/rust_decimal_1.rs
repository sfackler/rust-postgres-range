use rust_decimal_1::Decimal;

use crate::{BoundSided, Normalizable, RangeBound};

impl Normalizable for Decimal {
    fn normalize<S>(bound: RangeBound<S, Decimal>) -> RangeBound<S, Decimal>
    where
        S: BoundSided,
    {
        bound
    }
}
