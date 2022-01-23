
use std::sync::{Arc, Mutex, PoisonError};

use numpy::npyffi;
use pyo3::{AsPyPointer, ffi, prelude::*, types::{PyDict, PyList, PyTuple}};

use once_cell::sync::OnceCell;

mod dolphindb;

static PANDAS: OnceCell<PyObject> = OnceCell::new();

fn pandas(py: Python) -> PyResult<&PyAny> {
    PANDAS
        .get_or_try_init(|| Ok(py.import("pandas")?.into()))
        .map(|pandas| pandas.as_ref(py))
}

#[pyclass]
struct Connection {
    conn: Arc<futures::lock::Mutex<dolphindb::DBConnection>>,
    runtime: Arc<Mutex<tokio::runtime::Runtime>>,
}

#[pymethods]
impl Connection {
    pub fn run(&mut self, sql: String, _py: Python) -> PyResult<PyObject> {
        let client_clone = self.conn.clone();

        let rt = self.runtime.lock().unwrap_or_else(PoisonError::into_inner);
        let data = rt.block_on(async move {
            client_clone.lock().await.run(&sql).await
        }).unwrap();

        return match data {
            dolphindb::DataValue::Table(table) => {
                let mut names = Vec::new();
                let mut rows = Vec::new();

                for mut col in table.get_col().clone() {
                    names.push(col.get_name().clone());

                    if col.get_num_type() == dolphindb::DT_BOOL {
                        rows.push(list_from_bool(col.get_vector().get(), _py));
                    } else if col.get_num_type() == dolphindb::DT_STRING || col.get_num_type() == dolphindb::DT_SYMBOL {
                        rows.push(list_from_string(col.get_vector().get(), _py));
                    } else if col.get_num_type() == dolphindb::DT_INT {
                        rows.push(list_from_int(col.get_vector().get(), _py));
                    } else if col.get_num_type() == dolphindb::DT_LONG {
                        rows.push(list_from_long(col.get_vector().get(), _py));
                    } else if col.get_num_type() == dolphindb::DT_FLOAT {
                        rows.push(list_from_float(col.get_vector().get(), _py));
                    } else if col.get_num_type() == dolphindb::DT_DOUBLE {
                        rows.push(list_from_double(col.get_vector().get(), _py));
                    } else if col.get_num_type() == dolphindb::DT_DATE {
                        rows.push(list_from_date(col.get_vector().get(), _py));
                    } else if col.get_num_type() == dolphindb::DT_TIME {
                        rows.push(list_from_time(col.get_vector().get(), _py));
                    } else if col.get_num_type() == dolphindb::DT_MINUTE {
                        rows.push(list_from_minute(col.get_vector().get(), _py));
                    }
                }

                let dataframe_ = pandas(_py)?.getattr("DataFrame")?;

                let args = PyDict::new(_py);
                args.set_item("index", PyList::new(_py, 0..table.row_len()))?;
                args.set_item("columns", PyList::new(_py, vec![names.remove(0)]))?;
                let df = dataframe_.call(PyTuple::new(_py, vec![rows.remove(0)]), Some(args)).unwrap();

                for _ in 0..table.col_len() - 1 {
                    df.set_item(names.remove(0), rows.remove(0))?;
                }

                Ok(df.into())
            },
            _ => {
                println!("{:?}", data);
                Ok(PyList::empty(_py).into())
            }
        }
    }
}

fn list_from_bool(datas: &Vec<dolphindb::DataValue>, _py: Python) -> PyObject {
    return unsafe {
        let mut dtype: *mut numpy::npyffi::PyArray_Descr = std::ptr::null_mut();
        npyffi::PY_ARRAY_API.PyArray_DescrConverter("bool".into_py(_py).as_ptr(), &mut dtype);

        let mut length = datas.len() as isize;
        let array = numpy::PY_ARRAY_API.PyArray_NewFromDescr(
            numpy::PY_ARRAY_API.get_type_object(numpy::npyffi::array::NpyTypes::PyArray_Type),
            dtype,
            1_i32,
            &mut length as *mut numpy::npyffi::npy_intp,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0_i32,
            std::ptr::null_mut(),
        );

        for mut index in 0..length {
            let raw = match datas.get(index as usize).unwrap() { dolphindb::DataValue::Bool(b) => { *b }, _ => { false } };

            let row = numpy::PY_ARRAY_API.PyArray_GetPtr(
                array as *mut numpy::npyffi::PyArrayObject,
                &mut index as *mut numpy::npyffi::npy_intp,
            ) as *mut bool;
            *row = raw as bool;
        }

        PyObject::from_owned_ptr(_py, array as *mut pyo3::ffi::PyObject)
    }
}

fn list_from_string(datas: &Vec<dolphindb::DataValue>, _py: Python) -> PyObject {
    return unsafe {
        let mut dtype: *mut numpy::npyffi::PyArray_Descr = std::ptr::null_mut();
        npyffi::PY_ARRAY_API.PyArray_DescrConverter("object".into_py(_py).as_ptr(), &mut dtype);

        let mut length = datas.len() as isize;
        let array = numpy::PY_ARRAY_API.PyArray_NewFromDescr(
            numpy::PY_ARRAY_API.get_type_object(numpy::npyffi::array::NpyTypes::PyArray_Type),
            dtype,
            1_i32,
            &mut length as *mut numpy::npyffi::npy_intp,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0_i32,
            std::ptr::null_mut(),
        );

        for mut index in 0..length {
            let raw = match datas.get(index as usize).unwrap() { dolphindb::DataValue::Symbol(s) | dolphindb::DataValue::String(s) => { s.clone() }, _ => { "".to_string() } }.into_py(_py);

            let row = numpy::PY_ARRAY_API.PyArray_GetPtr(
                array as *mut numpy::npyffi::PyArrayObject,
                &mut index as *mut numpy::npyffi::npy_intp,
            ) as *mut &PyAny;

            ffi::Py_IncRef(raw.as_ptr());
            *row = raw.as_ref(_py);
        }

        PyObject::from_owned_ptr(_py, array as *mut pyo3::ffi::PyObject)
    }
}

fn list_from_int(datas: &Vec<dolphindb::DataValue>, _py: Python) -> PyObject {
    return unsafe {
        let mut dtype: *mut numpy::npyffi::PyArray_Descr = std::ptr::null_mut();
        npyffi::PY_ARRAY_API.PyArray_DescrConverter("int32".into_py(_py).as_ptr(), &mut dtype);

        let mut length = datas.len() as isize;
        let array = numpy::PY_ARRAY_API.PyArray_NewFromDescr(
            numpy::PY_ARRAY_API.get_type_object(numpy::npyffi::array::NpyTypes::PyArray_Type),
            dtype,
            1_i32,
            &mut length as *mut numpy::npyffi::npy_intp,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0_i32,
            std::ptr::null_mut(),
        );

        for mut index in 0..length {
            let raw = match datas.get(index as usize).unwrap() { dolphindb::DataValue::Int(n) => { *n }, _ => { 0 } } as i32;

            let row = numpy::PY_ARRAY_API.PyArray_GetPtr(
                array as *mut numpy::npyffi::PyArrayObject,
                &mut index as *mut numpy::npyffi::npy_intp,
            ) as *mut i32;
            *row = raw as i32;
        }

        PyObject::from_owned_ptr(_py, array as *mut pyo3::ffi::PyObject)
    }
}

fn list_from_long(datas: &Vec<dolphindb::DataValue>, _py: Python) -> PyObject {
    return unsafe {
        let mut dtype: *mut numpy::npyffi::PyArray_Descr = std::ptr::null_mut();
        npyffi::PY_ARRAY_API.PyArray_DescrConverter("int64".into_py(_py).as_ptr(), &mut dtype);

        let mut length = datas.len() as isize;
        let array = numpy::PY_ARRAY_API.PyArray_NewFromDescr(
            numpy::PY_ARRAY_API.get_type_object(numpy::npyffi::array::NpyTypes::PyArray_Type),
            dtype,
            1_i32,
            &mut length as *mut numpy::npyffi::npy_intp,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0_i32,
            std::ptr::null_mut(),
        );

        for mut index in 0..length {
            let raw = match datas.get(index as usize).unwrap() { dolphindb::DataValue::Long(n) => { *n }, _ => { 0 } } as i64;

            let row = numpy::PY_ARRAY_API.PyArray_GetPtr(
                array as *mut numpy::npyffi::PyArrayObject,
                &mut index as *mut numpy::npyffi::npy_intp,
            ) as *mut i64;
            *row = raw as i64;
        }

        PyObject::from_owned_ptr(_py, array as *mut pyo3::ffi::PyObject)
    }
}

fn list_from_float(datas: &Vec<dolphindb::DataValue>, _py: Python) -> PyObject {
    return unsafe {
        let mut dtype: *mut numpy::npyffi::PyArray_Descr = std::ptr::null_mut();
        npyffi::PY_ARRAY_API.PyArray_DescrConverter("float32".into_py(_py).as_ptr(), &mut dtype);

        let mut length = datas.len() as isize;
        let array = numpy::PY_ARRAY_API.PyArray_NewFromDescr(
            numpy::PY_ARRAY_API.get_type_object(numpy::npyffi::array::NpyTypes::PyArray_Type),
            dtype,
            1_i32,
            &mut length as *mut numpy::npyffi::npy_intp,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0_i32,
            std::ptr::null_mut(),
        );

        for mut index in 0..length {
            let raw = match datas.get(index as usize).unwrap() { dolphindb::DataValue::Float(n) => { *n }, _ => { 0.0 } } as f32;

            let row = numpy::PY_ARRAY_API.PyArray_GetPtr(
                array as *mut numpy::npyffi::PyArrayObject,
                &mut index as *mut numpy::npyffi::npy_intp,
            ) as *mut f32;
            *row = raw as f32;
        }

        PyObject::from_owned_ptr(_py, array as *mut pyo3::ffi::PyObject)
    }
}

fn list_from_double(datas: &Vec<dolphindb::DataValue>, _py: Python) -> PyObject {
    return unsafe {
        let mut dtype: *mut numpy::npyffi::PyArray_Descr = std::ptr::null_mut();
        npyffi::PY_ARRAY_API.PyArray_DescrConverter("float64".into_py(_py).as_ptr(), &mut dtype);

        let mut length = datas.len() as isize;
        let array = numpy::PY_ARRAY_API.PyArray_NewFromDescr(
            numpy::PY_ARRAY_API.get_type_object(numpy::npyffi::array::NpyTypes::PyArray_Type),
            dtype,
            1_i32,
            &mut length as *mut numpy::npyffi::npy_intp,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0_i32,
            std::ptr::null_mut(),
        );

        for mut index in 0..length {
            let raw = match datas.get(index as usize).unwrap() { dolphindb::DataValue::Double(n) => { *n }, _ => { 0.0 } } as f64;

            let row = numpy::PY_ARRAY_API.PyArray_GetPtr(
                array as *mut numpy::npyffi::PyArrayObject,
                &mut index as *mut numpy::npyffi::npy_intp,
            ) as *mut f64;
            *row = raw as f64;
        }

        PyObject::from_owned_ptr(_py, array as *mut pyo3::ffi::PyObject)
    }
}

fn list_from_date(datas: &Vec<dolphindb::DataValue>, _py: Python) -> PyObject {
    return unsafe {
        let mut dtype: *mut numpy::npyffi::PyArray_Descr = std::ptr::null_mut();
        npyffi::PY_ARRAY_API.PyArray_DescrConverter("datetime64[ns]".into_py(_py).as_ptr(), &mut dtype);

        let mut length = datas.len() as isize;
        let array = numpy::PY_ARRAY_API.PyArray_NewFromDescr(
            numpy::PY_ARRAY_API.get_type_object(numpy::npyffi::array::NpyTypes::PyArray_Type),
            dtype,
            1_i32,
            &mut length as *mut numpy::npyffi::npy_intp,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0_i32,
            std::ptr::null_mut(),
        );

        for mut index in 0..length {
            let mut raw = match datas.get(index as usize).unwrap() { dolphindb::DataValue::Date(n) => { n.raw }, _ => { 0 } } as i64;
            raw *= 86400000000000;

            let row = numpy::PY_ARRAY_API.PyArray_GetPtr(
                array as *mut numpy::npyffi::PyArrayObject,
                &mut index as *mut numpy::npyffi::npy_intp,
            ) as *mut i64;
            *row = raw as i64;
        }

        PyObject::from_owned_ptr(_py, array as *mut pyo3::ffi::PyObject)
    }
}

fn list_from_time(datas: &Vec<dolphindb::DataValue>, _py: Python) -> PyObject {
    return unsafe {
        let mut dtype: *mut numpy::npyffi::PyArray_Descr = std::ptr::null_mut();
        npyffi::PY_ARRAY_API.PyArray_DescrConverter("datetime64[ms]".into_py(_py).as_ptr(), &mut dtype);

        let mut length = datas.len() as isize;
        let array = numpy::PY_ARRAY_API.PyArray_NewFromDescr(
            numpy::PY_ARRAY_API.get_type_object(numpy::npyffi::array::NpyTypes::PyArray_Type),
            dtype,
            1_i32,
            &mut length as *mut numpy::npyffi::npy_intp,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0_i32,
            std::ptr::null_mut(),
        );

        for mut index in 0..length {
            let mut raw = match datas.get(index as usize).unwrap() { dolphindb::DataValue::Time(n) => { n.raw }, _ => { 0 } } as i64;
            raw *= 1000000;

            let row = numpy::PY_ARRAY_API.PyArray_GetPtr(
                array as *mut numpy::npyffi::PyArrayObject,
                &mut index as *mut numpy::npyffi::npy_intp,
            ) as *mut i64;
            *row = raw as i64;
        }

        PyObject::from_owned_ptr(_py, array as *mut pyo3::ffi::PyObject)
    }
}

fn list_from_minute(datas: &Vec<dolphindb::DataValue>, _py: Python) -> PyObject {
    return unsafe {
        let mut dtype: *mut numpy::npyffi::PyArray_Descr = std::ptr::null_mut();
        npyffi::PY_ARRAY_API.PyArray_DescrConverter("datetime64[m]".into_py(_py).as_ptr(), &mut dtype);

        let mut length = datas.len() as isize;
        let array = numpy::PY_ARRAY_API.PyArray_NewFromDescr(
            numpy::PY_ARRAY_API.get_type_object(numpy::npyffi::array::NpyTypes::PyArray_Type),
            dtype,
            1_i32,
            &mut length as *mut numpy::npyffi::npy_intp,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0_i32,
            std::ptr::null_mut(),
        );

        for mut index in 0..length {
            let mut raw = match datas.get(index as usize).unwrap() { dolphindb::DataValue::Time(n) => { n.raw }, _ => { 0 } } as i64;
            raw *= 60000000000;

            let row = numpy::PY_ARRAY_API.PyArray_GetPtr(
                array as *mut numpy::npyffi::PyArrayObject,
                &mut index as *mut numpy::npyffi::npy_intp,
            ) as *mut i64;
            *row = raw as i64;
        }

        PyObject::from_owned_ptr(_py, array as *mut pyo3::ffi::PyObject)
    }
}

#[pyfunction]
fn new_connection(ip: String, username: String, passwd: String) -> Connection {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();

    let conn = rt.block_on(async move {
        let mut r = dolphindb::DBConnection::new(ip).await;
        r.connect(username, passwd).await;
        r
    });

    Connection{
        conn: Arc::new(futures::lock::Mutex::new(conn)),
        runtime: Arc::new(Mutex::new(rt))
    }
}

#[pymodule]
fn dolphindb_client_pywrap(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(new_connection, m)?)?;

    pandas(_py).unwrap();
    Ok(())
}