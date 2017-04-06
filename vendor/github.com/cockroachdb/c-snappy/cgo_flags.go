// Package snappy uses the cgo compilation facilities to build the
// Snappy C++ library.
package snappy

// #cgo CXXFLAGS: -std=c++0x
// #cgo CPPFLAGS: -DHAVE_CONFIG_H -Iinternal
import "C"
