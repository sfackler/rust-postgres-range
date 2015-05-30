use std::io::prelude::*;
use std::error;
use postgres;
use postgres::types::{Type, Kind, ToSql, FromSql, IsNull, SessionInfo};
use postgres::error::Error;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use {Range, RangeBound, BoundType, BoundSided, Normalizable};

const RANGE_UPPER_UNBOUNDED: i8 = 0b0001_0000;
const RANGE_LOWER_UNBOUNDED: i8 = 0b0000_1000;
const RANGE_UPPER_INCLUSIVE: i8 = 0b0000_0100;
const RANGE_LOWER_INCLUSIVE: i8 = 0b0000_0010;
const RANGE_EMPTY: i8           = 0b0000_0001;

impl<T> FromSql for Range<T> where T: PartialOrd+Normalizable+FromSql {
    fn from_sql<R: Read>(ty: &Type, rdr: &mut R, info: &SessionInfo)
                         -> postgres::Result<Range<T>> {
        let element_type = match ty.kind() {
            &Kind::Range(ref ty) => ty,
            _ => panic!("unexpected type {:?}", ty)
        };

        let t = try!(rdr.read_i8());

        if t & RANGE_EMPTY != 0 {
            return Ok(Range::empty());
        }

        fn make_bound<S, T, R>(ty: &Type, rdr: &mut R, info: &SessionInfo,
                               tag: i8, bound_flag: i8, inclusive_flag: i8)
                               -> postgres::Result<Option<RangeBound<S, T>>>
                where S: BoundSided, T: PartialOrd+Normalizable+FromSql, R: Read {
            match tag & bound_flag {
                0 => {
                    let type_ = match tag & inclusive_flag {
                        0 => BoundType::Exclusive,
                        _ => BoundType::Inclusive,
                    };
                    let len = try!(rdr.read_i32::<BigEndian>()) as u64;
                    let mut limit = rdr.take(len);
                    let bound = try!(FromSql::from_sql(ty, &mut limit, info));
                    if limit.limit() != 0 {
                        let err: Box<error::Error+Sync+Send> =
                            "from_sql call did not consume all data".into();
                        return Err(Error::Conversion(err));
                    }
                    Ok(Some(RangeBound::new(bound, type_)))
                }
                _ => Ok(None)
            }
        }

        let lower = try!(make_bound(element_type, rdr, info, t,
                                    RANGE_LOWER_UNBOUNDED, RANGE_LOWER_INCLUSIVE));
        let upper = try!(make_bound(element_type, rdr, info, t,
                                    RANGE_UPPER_UNBOUNDED, RANGE_UPPER_INCLUSIVE));
        Ok(Range::new(lower, upper))
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            &Kind::Range(ref inner) => <T as FromSql>::accepts(inner),
            _ => false,
        }
    }
}

impl<T> ToSql for Range<T> where T: PartialOrd+Normalizable+ToSql {
    fn to_sql<W: ?Sized+Write>(&self, ty: &Type, mut buf: &mut W, info: &SessionInfo)
                               -> postgres::Result<IsNull> {
        let element_type = match ty.kind() {
            &Kind::Range(ref ty) => ty,
            _ => panic!("unexpected type {:?}", ty)
        };

        let mut tag = 0;
        if self.is_empty() {
            tag |= RANGE_EMPTY;
        } else {
            fn make_tag<S, T>(bound: Option<&RangeBound<S, T>>, unbounded_tag: i8,
                              inclusive_tag: i8) -> i8 where S: BoundSided {
                match bound {
                    None => unbounded_tag,
                    Some(&RangeBound { type_: BoundType::Inclusive, .. }) => inclusive_tag,
                    _ => 0
                }
            }
            tag |= make_tag(self.lower(), RANGE_LOWER_UNBOUNDED, RANGE_LOWER_INCLUSIVE);
            tag |= make_tag(self.upper(), RANGE_UPPER_UNBOUNDED, RANGE_UPPER_INCLUSIVE);
        }

        try!(buf.write_i8(tag));

        fn write_value<S, T, W: ?Sized>(ty: &Type, mut buf: &mut W, info: &SessionInfo,
                                        v: Option<&RangeBound<S, T>>) -> postgres::Result<()>
                where S: BoundSided, T: ToSql, W: Write {
            if let Some(bound) = v {
                let mut inner_buf = vec![];
                try!(bound.value.to_sql(ty, &mut inner_buf, info));
                try!(buf.write_u32::<BigEndian>(inner_buf.len() as u32));
                try!(buf.write_all(&*inner_buf));
            }
            Ok(())
        }

        try!(write_value(&element_type, buf, info, self.lower()));
        try!(write_value(&element_type, buf, info, self.upper()));

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

#[cfg(test)]
mod test {
    use std::fmt;

    use postgres::{Connection, SslMode};
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
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
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
