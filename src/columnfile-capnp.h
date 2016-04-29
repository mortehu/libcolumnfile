#ifndef CANTERA_COLUMNFILE_CAPNP_H_
#define CANTERA_COLUMNFILE_CAPNP_H_ 1

#include <capnp/dynamic.h>

#include "columnfile.h"

namespace cantera {

void WriteMessageToColumnFile(ColumnFileWriter& output,
                              capnp::DynamicValue::Reader message);

void ReadMessageFromColumnFile(ColumnFileReader& input,
                               capnp::DynamicValue::Builder output);

}  // namespace cantera

#endif  // !CANTERA_COLUMNFILE_CAPNP_H_
