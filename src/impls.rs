use std::error::Error;
use postgres::types::{Type, Kind, ToSql, FromSql, IsNull};
use postgres_protocol::{self as protocol, types};

use {Range, RangeBound, BoundType, BoundSided, Normalizable};

impl<T> FromSql for Range<T> where T: PartialOrd+Normalizable+FromSql {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Range<T>, Box<Error + Sync + Send>> {
        let element_type = match ty.kind() {
            &Kind::Range(ref ty) => ty,
            _ => panic!("unexpected type {:?}", ty)
        };

        match try!(types::range_from_sql(raw)) {
            types::Range::Empty => Ok(Range::empty()),
            types::Range::Nonempty(lower, upper) => {
                let lower = try!(bound_from_sql(lower, element_type));
                let upper = try!(bound_from_sql(upper, element_type));
                Ok(Range::new(lower, upper))
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            &Kind::Range(ref inner) => <T as FromSql>::accepts(inner),
            _ => false,
        }
    }
}

fn bound_from_sql<T, S>(bound: types::RangeBound<Option<&[u8]>>, ty: &Type) -> Result<Option<RangeBound<S, T>>, Box<Error + Sync + Send>>
    where T: PartialOrd + Normalizable + FromSql,
          S: BoundSided
{
    match bound {
        types::RangeBound::Exclusive(value) => {
            let value = match value {
                Some(value) => try!(T::from_sql(ty, value)),
                None => try!(T::from_sql_null(ty)),
            };
            Ok(Some(RangeBound::new(value, BoundType::Exclusive)))
        },
        types::RangeBound::Inclusive(value) => {
            let value = match value {
                Some(value) => try!(T::from_sql(ty, value)),
                None => try!(T::from_sql_null(ty)),
            };
            Ok(Some(RangeBound::new(value, BoundType::Inclusive)))
        },
        types::RangeBound::Unbounded => Ok(None),
    }
}

impl<T> ToSql for Range<T> where T: PartialOrd+Normalizable+ToSql {
    fn to_sql(&self, ty: &Type, buf: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        let element_type = match ty.kind() {
            &Kind::Range(ref ty) => ty,
            _ => panic!("unexpected type {:?}", ty)
        };

        if self.is_empty() {
            types::empty_range_to_sql(buf);
        } else {
            try!(types::range_to_sql(|buf| bound_to_sql(self.lower(), element_type, buf),
                                     |buf| bound_to_sql(self.upper(), element_type, buf),
                                     buf));
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            &Kind::Range(ref inner) => <T as ToSql>::accepts(inner),
            _ => false,
        }
    }

    to_sql_checked!();
}

fn bound_to_sql<S, T>(bound: Option<&RangeBound<S, T>>, ty: &Type, buf: &mut Vec<u8>) -> Result<types::RangeBound<protocol::IsNull>, Box<Error + Sync + Send>>
    where S: BoundSided,
          T: ToSql
{
    match bound {
        Some(bound) => {
            let null = match try!(bound.value.to_sql(ty, buf)) {
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

    use postgres::{Connection, TlsMode};
    use postgres::types::{FromSql, ToSql};
    use time::{self, Timespec};

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

    fn test_type<T: PartialEq+FromSql+ToSql, S: fmt::Display>(sql_type: &str, checks: &[(T, S)]) {
        let conn = Connection::connect("postgres://postgres@localhost", TlsMode::None).unwrap();
        for &(ref val, ref repr) in checks {
            let stmt = conn.prepare(&*format!("SELECT {}::{}", *repr, sql_type)).unwrap();
            let result = stmt.query(&[]).unwrap().iter().next().unwrap().get(0);
            assert!(val == &result);

            let stmt = conn.prepare(&*format!("SELECT $1::{}", sql_type)).unwrap();
            let result = stmt.query(&[val]).unwrap().iter().next().unwrap().get(0);
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

    fn test_timespec_range_params(sql_type: &str) {
        fn t(time: &str) -> Timespec {
            time::strptime(time, "%Y-%m-%d").unwrap().to_timespec()
        }
        let low = "1970-01-01";
        let high = "1980-01-01";
        test_range!(sql_type, Timespec, t(low), low, t(high), high);
    }

    #[test]
    fn test_tsrange_params() {
        test_timespec_range_params("TSRANGE");
    }

    #[test]
    fn test_tstzrange_params() {
        test_timespec_range_params("TSTZRANGE");
    }
}
