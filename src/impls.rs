use postgres_protocol::{self as protocol, types};
use postgres_types::private::BytesMut;
use postgres_types::{FromSql, IsNull, Kind, ToSql, Type};
use std::error::Error;

use crate::{BoundSided, BoundType, Normalizable, Range, RangeBound};

impl<'a, T> FromSql<'a> for Range<T>
where
    T: PartialOrd + Normalizable + FromSql<'a>,
{
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Range<T>, Box<dyn Error + Sync + Send>> {
        let element_type = match *ty.kind() {
            Kind::Range(ref ty) => ty,
            _ => panic!("unexpected type {:?}", ty),
        };

        match types::range_from_sql(raw)? {
            types::Range::Empty => Ok(Range::empty()),
            types::Range::Nonempty(lower, upper) => {
                let lower = bound_from_sql(lower, element_type)?;
                let upper = bound_from_sql(upper, element_type)?;
                Ok(Range::new(lower, upper))
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Range(ref inner) => <T as FromSql>::accepts(inner),
            _ => false,
        }
    }
}

fn bound_from_sql<'a, T, S>(
    bound: types::RangeBound<Option<&'a [u8]>>,
    ty: &Type,
) -> Result<Option<RangeBound<S, T>>, Box<dyn Error + Sync + Send>>
where
    T: PartialOrd + Normalizable + FromSql<'a>,
    S: BoundSided,
{
    match bound {
        types::RangeBound::Exclusive(value) => {
            let value = match value {
                Some(value) => T::from_sql(ty, value)?,
                None => T::from_sql_null(ty)?,
            };
            Ok(Some(RangeBound::new(value, BoundType::Exclusive)))
        }
        types::RangeBound::Inclusive(value) => {
            let value = match value {
                Some(value) => T::from_sql(ty, value)?,
                None => T::from_sql_null(ty)?,
            };
            Ok(Some(RangeBound::new(value, BoundType::Inclusive)))
        }
        types::RangeBound::Unbounded => Ok(None),
    }
}

impl<T> ToSql for Range<T>
where
    T: PartialOrd + Normalizable + ToSql,
{
    fn to_sql(
        &self,
        ty: &Type,
        buf: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let element_type = match *ty.kind() {
            Kind::Range(ref ty) => ty,
            _ => panic!("unexpected type {:?}", ty),
        };

        if self.is_empty() {
            types::empty_range_to_sql(buf);
        } else {
            types::range_to_sql(
                |buf| bound_to_sql(self.lower(), element_type, buf),
                |buf| bound_to_sql(self.upper(), element_type, buf),
                buf,
            )?;
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Range(ref inner) => <T as ToSql>::accepts(inner),
            _ => false,
        }
    }

    to_sql_checked!();
}

fn bound_to_sql<S, T>(
    bound: Option<&RangeBound<S, T>>,
    ty: &Type,
    buf: &mut BytesMut,
) -> Result<types::RangeBound<protocol::IsNull>, Box<dyn Error + Sync + Send>>
where
    S: BoundSided,
    T: ToSql,
{
    match bound {
        Some(bound) => {
            let null = match bound.value.to_sql(ty, buf)? {
                IsNull::Yes => protocol::IsNull::Yes,
                IsNull::No => protocol::IsNull::No,
            };

            match bound.type_ {
                BoundType::Exclusive => Ok(types::RangeBound::Exclusive(null)),
                BoundType::Inclusive => Ok(types::RangeBound::Inclusive(null)),
            }
        }
        None => Ok(types::RangeBound::Unbounded),
    }
}

#[cfg(test)]
mod test {
    use std::fmt;

    #[cfg(feature = "with-chrono-0_4")]
    use chrono_04::{Duration, TimeZone, Utc};
    use postgres::types::{FromSql, ToSql};
    use postgres::{Client, NoTls};
    #[cfg(feature = "with-rust_decimal-1")]
    use rust_decimal_1::Decimal;

    macro_rules! test_range {
        ($name:expr, $t:ty, $low:expr, $low_str:expr, $high:expr, $high_str:expr) => ({
            let tests = &[(Some(range!('(',; ')')), "'(,)'".to_string()),
                         (Some(range!('[' $low,; ')')), format!("'[{},)'", $low_str)),
                         (Some(range!('(' $low,; ')')), format!("'({},)'", $low_str)),
                         (Some(range!('(', $high; ']')), format!("'(,{}]'", $high_str)),
                         (Some(range!('(', $high; ')')), format!("'(,{})'", $high_str)),
                         (Some(range!('[' $low, $high; ']')),
                          format!("'[{},{}]'", $low_str, $high_str)),
                         (Some(range!('[' $low, $high; ')')),
                          format!("'[{},{})'", $low_str, $high_str)),
                         (Some(range!('(' $low, $high; ']')),
                          format!("'({},{}]'", $low_str, $high_str)),
                         (Some(range!('(' $low, $high; ')')),
                          format!("'({},{})'", $low_str, $high_str)),
                         (Some(range!(empty)), "'empty'".to_string()),
                         (None, "NULL".to_string())];
            test_type($name, tests);
        })
    }

    fn test_type<T, S>(sql_type: &str, checks: &[(T, S)])
    where
        for<'a> T: Sync + PartialEq + FromSql<'a> + ToSql,
        S: fmt::Display,
    {
        let mut conn = Client::connect("postgres://postgres@localhost", NoTls).unwrap();
        for &(ref val, ref repr) in checks {
            let stmt = conn
                .prepare(&*format!("SELECT {}::{}", *repr, sql_type))
                .unwrap();
            let result = conn
                .query(&stmt, &[])
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .get(0);
            assert!(val == &result);

            let stmt = conn.prepare(&*format!("SELECT $1::{}", sql_type)).unwrap();
            let result = conn
                .query(&stmt, &[val])
                .unwrap()
                .iter()
                .next()
                .unwrap()
                .get(0);
            assert!(val == &result);
        }
    }

    #[test]
    fn test_int4range_params() {
        test_range!("INT4RANGE", i32, 100i32, "100", 200i32, "200")
    }

    #[test]
    fn test_int8range_params() {
        test_range!("INT8RANGE", i64, 100i64, "100", 200i64, "200")
    }

    #[test]
    #[cfg(feature = "with-rust_decimal-1")]
    fn test_numrange_params() {
        let low = Decimal::new(202, 2);
        let high = Decimal::new(202, 1);
        test_range!("NUMRANGE", Decimal, low, "2.02", high, "20.2");
    }

    #[test]
    #[cfg(feature = "with-chrono-0_4")]
    fn test_tsrange_params() {
        let low = Utc.timestamp_opt(0, 0).unwrap();
        let high = low + Duration::days(10);
        test_range!(
            "TSRANGE",
            NaiveDateTime,
            low.naive_utc(),
            "1970-01-01",
            high.naive_utc(),
            "1970-01-11"
        );
    }

    #[test]
    #[cfg(feature = "with-chrono-0_4")]
    fn test_tstzrange_params() {
        let low = Utc.timestamp_opt(0, 0).unwrap();
        let high = low + Duration::days(10);
        test_range!(
            "TSTZRANGE",
            DateTime<Utc>,
            low,
            "1970-01-01",
            high,
            "1970-01-11"
        );
    }
}
