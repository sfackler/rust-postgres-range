use std::error::Error;
use postgres_types::{FromSql, IsNull, Kind, ToSql, Type};
use postgres_types::private::BytesMut;
use postgres_protocol::{self as protocol, types};

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

fn bound_from_sql<'a, T, S>(bound: types::RangeBound<Option<&'a [u8]>>, ty: &Type) -> Result<Option<RangeBound<S, T>>, Box<dyn Error + Sync + Send>>
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
    fn to_sql(&self, ty: &Type, buf: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
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

fn bound_to_sql<S, T>(bound: Option<&RangeBound<S, T>>, ty: &Type, buf: &mut BytesMut) -> Result<types::RangeBound<protocol::IsNull>, Box<dyn Error + Sync + Send>>
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

    use postgres::{Client, NoTls};
    use postgres::types::{FromSql, ToSql};
    #[cfg(feature = "with-chrono-0_4")]
    use chrono_04::{NaiveDate, NaiveTime, NaiveDateTime, TimeZone, Utc, Duration};

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
    where for<'a>
        T: Sync + PartialEq + FromSql<'a> + ToSql + fmt::Debug,
        S: fmt::Display
    {
        let mut conn = Client::connect("postgres://postgres@localhost", NoTls).unwrap();
        for &(ref val, ref repr) in checks {
            let stmt = conn.prepare(&*format!("SELECT {}::{}", *repr, sql_type))
                .unwrap();
            let result = conn.query(&stmt, &[]).unwrap().iter().next().unwrap().get(0);
            assert_eq!(val, &result, "'SELECT {repr}::{sql_type}'");

            let stmt = conn.prepare(&*format!("SELECT $1::{}", sql_type)).unwrap();
            let result = conn.query(&stmt, &[val]).unwrap().iter().next().unwrap().get(0);
            assert_eq!(val, &result, "'SELECT $1::{sql_type}'");
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
    #[cfg(feature = "with-chrono-0_4")]
    fn test_tsrange_params() {
        let d = NaiveDate::from_ymd_opt(2015, 6, 3).unwrap();
        let t = NaiveTime::from_hms_milli_opt(12, 34, 56, 789).unwrap();

        let low = NaiveDateTime::new(d, t);
        let high = low + Duration::days(10);
        test_range!("TSRANGE", NaiveDateTime, low, "2015-06-03T12:34:56.789", high, "2015-06-13T12:34:56.789");
    }

    #[test]
    #[cfg(feature = "with-chrono-0_4")]
    fn test_tstzrange_params() {
        let low = Utc.with_ymd_and_hms(2014, 7, 8, 9, 10, 11).unwrap();
        let high = low + Duration::days(10);
        test_range!("TSTZRANGE", DateTime<_>, low, "2014-07-08T09:10:11Z", high, "2014-07-18T09:10:11Z");
    }


    #[test]
    #[cfg(feature = "with-chrono-0_4")]
    fn test_daterange_params() {
        let low = NaiveDate::from_ymd_opt(2015, 6, 4).unwrap();
        let high = low + Duration::days(10);
        test_range!("DATERANGE", NaiveDate, low, "2015-06-04", high, "2015-06-14");
    }
}
