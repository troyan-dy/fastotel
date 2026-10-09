use pyo3::prelude::*;

/// The native part of fastotel, imported as `fastotel._fastotel`.
#[pymodule(gil_used = false)]
mod _fastotel {}
