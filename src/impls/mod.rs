use std::io::ByRefReader;
use std::io::util::LimitReader;

use time::Timespec;
use postgres::Type;
use postgres::types::{RawFromSql, RawToSql};

use {postgres, Range, RangeBound, BoundType, BoundSided, Normalizable};

macro_rules! check_types {
    ($($expected:pat)|+, $actual:ident) => (
        match $actual {
            $(&$expected)|+ => {}
            actual => return Err(::postgres::Error::WrongType(actual.clone()))
        }
    )
}

macro_rules! from_sql_impl {
    ($($oid:pat)|+, $t:ty) => {
        impl ::postgres::FromSql for Option<::Range<$t>> {
            fn from_sql(ty: &::postgres::Type, raw: Option<&[u8]>) -> ::postgres::Result<Self> {
                check_types!($($oid)|+, ty);

                match raw {
                    Some(mut raw) => ::postgres::types::RawFromSql::raw_from_sql(&mut raw).map(Some),
                    None => Ok(None),
                }
            }
        }

        impl ::postgres::FromSql for ::Range<$t> {
            fn from_sql(ty: &::postgres::Type, raw: Option<&[u8]>) -> ::postgres::Result<Self> {
                let v: ::postgres::Result<Option<Self>> = ::postgres::FromSql::from_sql(ty, raw);
                match v {
                    Ok(None) => Err(::postgres::Error::WasNull),
                    Ok(Some(v)) => Ok(v),
                    Err(err) => Err(err),
                }
            }
        }
    }
}

macro_rules! to_sql_impl {
    ($($oid:pat)|+, $t:ty) => {
        impl ::postgres::ToSql for ::Range<$t> {
            fn to_sql(&self, ty: &::postgres::Type) -> ::postgres::Result<Option<Vec<u8>>> {
                check_types!($($oid)|+, ty);

                let mut writer = vec![];
                try!(self.raw_to_sql(&mut writer));
                Ok(Some(writer))
            }
        }

        impl ::postgres::ToSql for Option<::Range<$t>> {
            fn to_sql(&self, ty: &::postgres::Type) -> ::postgres::Result<Option<Vec<u8>>> {
                check_types!($($oid)|+, ty);
                match *self {
                    Some(ref arr) => arr.to_sql(ty),
                    None => Ok(None)
                }
            }
        }
    }
}

const RANGE_UPPER_UNBOUNDED: i8 = 0b0001_0000;
const RANGE_LOWER_UNBOUNDED: i8 = 0b0000_1000;
const RANGE_UPPER_INCLUSIVE: i8 = 0b0000_0100;
const RANGE_LOWER_INCLUSIVE: i8 = 0b0000_0010;
const RANGE_EMPTY: i8           = 0b0000_0001;

impl<T> RawFromSql for Range<T> where T: PartialOrd+Normalizable+RawFromSql {
    fn raw_from_sql<R: Reader>(rdr: &mut R) -> postgres::Result<Range<T>> {
        let t = try!(rdr.read_i8());

        if t & RANGE_EMPTY != 0 {
            return Ok(Range::empty());
        }

        fn make_bound<S, T, R>(rdr: &mut R, tag: i8, bound_flag: i8, inclusive_flag: i8)
                               -> postgres::Result<Option<RangeBound<S, T>>>
                where S: BoundSided, T: PartialOrd+Normalizable+RawFromSql, R: Reader {
            match tag & bound_flag {
                0 => {
                    let type_ = match tag & inclusive_flag {
                        0 => BoundType::Exclusive,
                        _ => BoundType::Inclusive,
                    };
                    let len = try!(rdr.read_be_i32()) as uint;
                    let mut limit = LimitReader::new(rdr.by_ref(), len);
                    let bound = try!(RawFromSql::raw_from_sql(&mut limit));
                    if limit.limit() != 0 {
                        return Err(postgres::Error::BadData);
                    }
                    Ok(Some(RangeBound::new(bound, type_)))
                }
                _ => Ok(None)
            }
        }

        let lower = try!(make_bound(rdr, t, RANGE_LOWER_UNBOUNDED, RANGE_LOWER_INCLUSIVE));
        let upper = try!(make_bound(rdr, t, RANGE_UPPER_UNBOUNDED, RANGE_UPPER_INCLUSIVE));
        Ok(Range::new(lower, upper))
    }
}

from_sql_impl!(Type::Int4Range, i32);
from_sql_impl!(Type::Int8Range, i64);
from_sql_impl!(Type::TsRange | Type::TstzRange, Timespec);

impl<T> RawToSql for Range<T> where T: PartialOrd+Normalizable+RawToSql {
    fn raw_to_sql<W: Writer>(&self, buf: &mut W) -> postgres::Result<()> {
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

        fn write_value<S, T, W>(buf: &mut W, v: Option<&RangeBound<S, T>>) -> postgres::Result<()>
                where S: BoundSided, T: RawToSql, W: Writer {
            if let Some(bound) = v {
                let mut inner_buf = vec![];
                try!(bound.value.raw_to_sql(&mut inner_buf));
                try!(buf.write_be_u32(inner_buf.len() as u32));
                try!(buf.write(&*inner_buf));
            }
            Ok(())
        }

        try!(write_value(buf, self.lower()));
        try!(write_value(buf, self.upper()));

        Ok(())
    }
}

to_sql_impl!(Type::Int4Range, i32);
to_sql_impl!(Type::Int8Range, i64);
to_sql_impl!(Type::TsRange | Type::TstzRange, Timespec);

#[cfg(test)]
mod test {
    use std::fmt;

    use postgres::{Connection, FromSql, ToSql, SslMode};
    use time::{mod, Timespec};

    macro_rules! test_range {
        ($name:expr, $t:ty, $low:expr, $low_str:expr, $high:expr, $high_str:expr) => ({
            let tests = &[(Some(range!('(', ')')), "'(,)'".to_string()),
                         (Some(range!('[' $low, ')')), format!("'[{},)'", $low_str)),
                         (Some(range!('(' $low, ')')), format!("'({},)'", $low_str)),
                         (Some(range!('(', $high ']')), format!("'(,{}]'", $high_str)),
                         (Some(range!('(', $high ')')), format!("'(,{})'", $high_str)),
                         (Some(range!('[' $low, $high ']')),
                          format!("'[{},{}]'", $low_str, $high_str)),
                         (Some(range!('[' $low, $high ')')),
                          format!("'[{},{})'", $low_str, $high_str)),
                         (Some(range!('(' $low, $high ']')),
                          format!("'({},{}]'", $low_str, $high_str)),
                         (Some(range!('(' $low, $high ')')),
                          format!("'({},{})'", $low_str, $high_str)),
                         (Some(range!(empty)), "'empty'".to_string()),
                         (None, "NULL".to_string())];
            test_type($name, tests);
        })
    }

    fn test_type<T: PartialEq+FromSql+ToSql, S: fmt::Show>(sql_type: &str, checks: &[(T, S)]) {
        let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
        for &(ref val, ref repr) in checks.iter() {
            let stmt = conn.prepare(&*format!("SELECT {}::{}", *repr, sql_type)).unwrap();
            let result = stmt.query(&[]).unwrap().next().unwrap().get(0u);
            assert!(val == &result);

            let stmt = conn.prepare(&*format!("SELECT $1::{}", sql_type)).unwrap();
            let result = stmt.query(&[val]).unwrap().next().unwrap().get(0u);
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
