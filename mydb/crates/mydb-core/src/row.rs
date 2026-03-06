use std::str;

use crate::{DataType, MyDbError, Result, Schema, Value};

const ROW_VERSION: u8 = 1;
const HEADER_SIZE: usize = 9;
const OFFSET_ENTRY_SIZE: usize = 4;

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn encode(&self, schema: &Schema) -> Result<Vec<u8>> {
        if self.values.len() != schema.len() {
            return Err(MyDbError::InvalidSchema(format!(
                "row value count {} does not match schema columns {}",
                self.values.len(),
                schema.len()
            )));
        }
        if schema.len() > u16::MAX as usize {
            return Err(MyDbError::InvalidSchema(
                "schema has too many columns".to_string(),
            ));
        }

        let null_bitmap_len = schema.null_bitmap_len();
        let mut null_bitmap = vec![0u8; null_bitmap_len];
        let mut fixed_area = Vec::with_capacity(schema.fixed_area_len());
        let mut var_offsets = Vec::<(u16, u16)>::with_capacity(schema.variable_column_count());
        let mut var_data = Vec::<u8>::new();

        for (index, (column, value)) in schema.columns.iter().zip(&self.values).enumerate() {
            if matches!(value, Value::Null) {
                if !column.nullable {
                    return Err(MyDbError::NullConstraintViolation {
                        column: column.name.clone(),
                    });
                }
                set_null_bit(&mut null_bitmap, index, true);
                match column.data_type {
                    DataType::Int => fixed_area.extend_from_slice(&0i32.to_le_bytes()),
                    DataType::BigInt => fixed_area.extend_from_slice(&0i64.to_le_bytes()),
                    DataType::Bool => fixed_area.push(0),
                    DataType::Double => fixed_area.extend_from_slice(&0f64.to_le_bytes()),
                    DataType::Varchar => var_offsets.push((0, 0)),
                }
                continue;
            }

            match (column.data_type, value) {
                (DataType::Int, Value::Int(v)) => fixed_area.extend_from_slice(&v.to_le_bytes()),
                (DataType::BigInt, Value::BigInt(v)) => {
                    fixed_area.extend_from_slice(&v.to_le_bytes())
                }
                (DataType::Bool, Value::Bool(v)) => fixed_area.push(u8::from(*v)),
                (DataType::Double, Value::Double(v)) => {
                    fixed_area.extend_from_slice(&v.to_le_bytes())
                }
                (DataType::Varchar, Value::Varchar(v)) => {
                    let start = var_data.len();
                    let len = v.len();
                    if start > u16::MAX as usize || len > u16::MAX as usize {
                        return Err(MyDbError::RecordTooLarge { size: start + len });
                    }
                    var_offsets.push((start as u16, len as u16));
                    var_data.extend_from_slice(v.as_bytes());
                }
                (expected, actual) => {
                    return Err(MyDbError::TypeMismatch {
                        column: column.name.clone(),
                        expected: expected.as_str().to_string(),
                        actual: actual.type_name().to_string(),
                    });
                }
            }
        }

        let fixed_len_u16 =
            u16::try_from(fixed_area.len()).map_err(|_| MyDbError::RecordTooLarge {
                size: fixed_area.len(),
            })?;
        let null_len_u16 =
            u16::try_from(null_bitmap_len).map_err(|_| MyDbError::RecordTooLarge {
                size: null_bitmap_len,
            })?;
        let var_count_u16 =
            u16::try_from(var_offsets.len()).map_err(|_| MyDbError::RecordTooLarge {
                size: var_offsets.len(),
            })?;

        let mut out = Vec::with_capacity(
            HEADER_SIZE
                + null_bitmap.len()
                + fixed_area.len()
                + var_offsets.len() * OFFSET_ENTRY_SIZE
                + var_data.len(),
        );
        out.push(ROW_VERSION);
        out.extend_from_slice(&(schema.len() as u16).to_le_bytes());
        out.extend_from_slice(&null_len_u16.to_le_bytes());
        out.extend_from_slice(&fixed_len_u16.to_le_bytes());
        out.extend_from_slice(&var_count_u16.to_le_bytes());
        out.extend_from_slice(&null_bitmap);
        out.extend_from_slice(&fixed_area);
        for (start, len) in var_offsets {
            out.extend_from_slice(&start.to_le_bytes());
            out.extend_from_slice(&len.to_le_bytes());
        }
        out.extend_from_slice(&var_data);
        Ok(out)
    }

    pub fn decode(schema: &Schema, bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(MyDbError::Corruption("row payload too short".to_string()));
        }

        if bytes[0] != ROW_VERSION {
            return Err(MyDbError::Corruption(format!(
                "unsupported row version {}",
                bytes[0]
            )));
        }

        let column_count = read_u16(bytes, 1)? as usize;
        let null_bitmap_len = read_u16(bytes, 3)? as usize;
        let fixed_len = read_u16(bytes, 5)? as usize;
        let var_count = read_u16(bytes, 7)? as usize;

        if column_count != schema.len() {
            return Err(MyDbError::Corruption(format!(
                "row column count {} does not match schema {}",
                column_count,
                schema.len()
            )));
        }
        if null_bitmap_len != schema.null_bitmap_len() {
            return Err(MyDbError::Corruption(format!(
                "row null bitmap len {} does not match schema {}",
                null_bitmap_len,
                schema.null_bitmap_len()
            )));
        }
        if fixed_len != schema.fixed_area_len() {
            return Err(MyDbError::Corruption(format!(
                "row fixed len {} does not match schema {}",
                fixed_len,
                schema.fixed_area_len()
            )));
        }
        if var_count != schema.variable_column_count() {
            return Err(MyDbError::Corruption(format!(
                "row var count {} does not match schema {}",
                var_count,
                schema.variable_column_count()
            )));
        }

        let null_start = HEADER_SIZE;
        let fixed_start = null_start + null_bitmap_len;
        let var_offset_start = fixed_start + fixed_len;
        let var_offset_len = var_count * OFFSET_ENTRY_SIZE;
        let var_data_start = var_offset_start + var_offset_len;
        if bytes.len() < var_data_start {
            return Err(MyDbError::Corruption("row payload truncated".to_string()));
        }

        let null_bitmap = &bytes[null_start..fixed_start];
        let fixed_area = &bytes[fixed_start..var_offset_start];
        let var_offset_bytes = &bytes[var_offset_start..var_data_start];
        let var_data = &bytes[var_data_start..];

        let mut var_entries = Vec::with_capacity(var_count);
        for i in 0..var_count {
            let start = i * OFFSET_ENTRY_SIZE;
            let offset =
                u16::from_le_bytes([var_offset_bytes[start], var_offset_bytes[start + 1]]) as usize;
            let len = u16::from_le_bytes([var_offset_bytes[start + 2], var_offset_bytes[start + 3]])
                as usize;
            var_entries.push((offset, len));
        }

        let mut values = Vec::with_capacity(schema.len());
        let mut fixed_cursor = 0usize;
        let mut var_cursor = 0usize;
        for (index, column) in schema.columns.iter().enumerate() {
            let is_null = null_bit_is_set(null_bitmap, index);
            match column.data_type {
                DataType::Int => {
                    let raw = read_slice(fixed_area, &mut fixed_cursor, 4)?;
                    if is_null {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::Int(i32::from_le_bytes(raw.try_into().unwrap())));
                    }
                }
                DataType::BigInt => {
                    let raw = read_slice(fixed_area, &mut fixed_cursor, 8)?;
                    if is_null {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::BigInt(i64::from_le_bytes(raw.try_into().unwrap())));
                    }
                }
                DataType::Bool => {
                    let raw = read_slice(fixed_area, &mut fixed_cursor, 1)?;
                    if is_null {
                        values.push(Value::Null);
                    } else {
                        let value = match raw[0] {
                            0 => false,
                            1 => true,
                            other => {
                                return Err(MyDbError::Corruption(format!(
                                    "invalid bool payload byte {other}"
                                )));
                            }
                        };
                        values.push(Value::Bool(value));
                    }
                }
                DataType::Double => {
                    let raw = read_slice(fixed_area, &mut fixed_cursor, 8)?;
                    if is_null {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::Double(f64::from_le_bytes(raw.try_into().unwrap())));
                    }
                }
                DataType::Varchar => {
                    if var_cursor >= var_entries.len() {
                        return Err(MyDbError::Corruption(
                            "varchar offset table underflow".to_string(),
                        ));
                    }
                    let (offset, len) = var_entries[var_cursor];
                    var_cursor += 1;
                    if is_null {
                        values.push(Value::Null);
                    } else {
                        let end = offset + len;
                        if end > var_data.len() {
                            return Err(MyDbError::Corruption(format!(
                                "varchar range {}..{} out of bounds {}",
                                offset,
                                end,
                                var_data.len()
                            )));
                        }
                        let s = str::from_utf8(&var_data[offset..end]).map_err(|err| {
                            MyDbError::Corruption(format!("invalid utf8 varchar payload: {err}"))
                        })?;
                        values.push(Value::Varchar(s.to_string()));
                    }
                }
            }
        }

        Ok(Self { values })
    }
}

fn set_null_bit(bitmap: &mut [u8], index: usize, is_null: bool) {
    let byte_idx = index / 8;
    let bit_idx = index % 8;
    if is_null {
        bitmap[byte_idx] |= 1 << bit_idx;
    } else {
        bitmap[byte_idx] &= !(1 << bit_idx);
    }
}

fn null_bit_is_set(bitmap: &[u8], index: usize) -> bool {
    let byte_idx = index / 8;
    let bit_idx = index % 8;
    bitmap[byte_idx] & (1 << bit_idx) != 0
}

fn read_u16(bytes: &[u8], start: usize) -> Result<u16> {
    if start + 2 > bytes.len() {
        return Err(MyDbError::Corruption(
            "unexpected end of row payload".to_string(),
        ));
    }
    Ok(u16::from_le_bytes([bytes[start], bytes[start + 1]]))
}

fn read_slice<'a>(src: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = *cursor + len;
    if end > src.len() {
        return Err(MyDbError::Corruption(
            "fixed area length mismatch".to_string(),
        ));
    }
    let range = &src[*cursor..end];
    *cursor = end;
    Ok(range)
}

#[cfg(test)]
mod tests {
    use crate::{Column, DataType, Schema, Value};

    use super::Row;

    #[test]
    fn row_roundtrip_with_nulls_and_varchar() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Int, false),
            Column::new("name", DataType::Varchar, false),
            Column::new("score", DataType::Double, true),
            Column::new("ok", DataType::Bool, true),
        ])
        .unwrap();
        let row = Row::new(vec![
            Value::Int(7),
            Value::Varchar("alice".to_string()),
            Value::Null,
            Value::Bool(true),
        ]);

        let bytes = row.encode(&schema).unwrap();
        let decoded = Row::decode(&schema, &bytes).unwrap();
        assert_eq!(decoded, row);
    }
}
