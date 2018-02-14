package pb2json

import (
	"fmt"

	"log"

	"strings"

	"io"

	"bytes"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
)

func getNameByID(ID int32, desc *descriptor.DescriptorProto) string {
	for _, f := range desc.GetField() {
		if f.GetNumber() == ID {
			return f.GetJsonName()
		}
	}
	return ""
}

func getFieldDescriptorByID(ID int32, desc *descriptor.DescriptorProto) *descriptor.FieldDescriptorProto {
	for _, f := range desc.GetField() {
		if f.GetNumber() == ID {
			return f
		}
	}
	return nil
}

func findMessageDescriptor(fileDesc *descriptor.FileDescriptorProto, fullTypeName string) *descriptor.DescriptorProto {

	//log.Printf("got name %s", fullTypeName)
	typeName := strings.TrimPrefix(fullTypeName, "."+fileDesc.GetPackage()+".")
	//log.Printf("trimmed name %s", typeName)
	typeSubstrings := strings.Split(typeName, ".")
	//log.Printf("splitted %s", typeSubstrings)

	var upperLevel *descriptor.DescriptorProto

	for _, mdesc := range fileDesc.GetMessageType() {
		//log.Printf("searching global type %s", mdesc.GetName())
		if mdesc.GetName() == typeSubstrings[0] {
			upperLevel = mdesc
		}
	}

	if upperLevel == nil {
		return nil
	}

	if len(typeSubstrings) == 1 {
		//log.Printf("returning %s", upperLevel.GetName())
		return upperLevel
	}

	typeSubstrings = typeSubstrings[1:]
	current := upperLevel

cont:
	for len(typeSubstrings) > 0 {
		for _, nested := range current.GetNestedType() {
			if nested.GetName() == typeSubstrings[0] {
				current = nested
				typeSubstrings = typeSubstrings[1:]
				break cont
			}
		}
	}

	//log.Printf("returning %s", current.GetName())

	return current
}

func unmarshalMessage(buffer *proto.Buffer, length int, depth int,
	fileDesc *descriptor.FileDescriptorProto, msgDesc *descriptor.DescriptorProto) ([]byte, error) {

	var u uint64
	var output bytes.Buffer
	var i int

	output.WriteString("{")

out:
	for {

		op, err := buffer.DecodeVarint()
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				break out
			}

			fmt.Printf("fetching op err %v\n", err)
			break out
		}

		if i != 0 {
			output.WriteString(",")
		}
		i++

		tag := op >> 3
		wire := op & 7

		switch wire {
		default:
			fmt.Printf("t=%3d unknown wire=%d\n", tag, wire)
			break out

		case proto.WireBytes:
			var r []byte

			r, err = buffer.DecodeRawBytes(false)
			if err != nil {
				break out
			}

			fdesc := getFieldDescriptorByID(int32(tag), msgDesc)

			if fdesc.GetType() == descriptor.FieldDescriptorProto_TYPE_MESSAGE {

				fmt.Fprintf(&output, "\"%s\": ", fdesc.GetJsonName())

				smDesc := findMessageDescriptor(fileDesc, fdesc.GetTypeName())
				if smDesc == nil {
					log.Fatal("not found descriptor for type %s", fdesc.GetTypeName())
				}
				r, err := unmarshalMessage(proto.NewBuffer(r), len(r), depth+1, fileDesc, smDesc)
				if err != nil {
					return []byte{}, err
				}
				output.Write(r)

			} else {
				fmt.Printf("t=%3d name=%s bytes [%d]", tag, getNameByID(int32(tag), msgDesc), len(r))

				if len(r) <= 6 {
					for i := 0; i < len(r); i++ {
						fmt.Printf(" %.2x", r[i])
					}
				} else {
					for i := 0; i < 3; i++ {
						fmt.Printf(" %.2x", r[i])
					}
					fmt.Printf(" ..")
					for i := len(r) - 3; i < len(r); i++ {
						fmt.Printf(" %.2x", r[i])
					}
				}
				fmt.Printf("\n")
			}

		case proto.WireFixed32:
			u, err = buffer.DecodeFixed32()
			if err != nil {
				fmt.Printf("t=%3d name=%s fix32 err %v\n", tag, getNameByID(int32(tag), msgDesc), err)
				break out
			}
			fmt.Fprintf(&output, "\"%s\": %d", getNameByID(int32(tag), msgDesc), u)
			//fmt.Printf("t=%3d name=%s fix32 %d\n", tag, getNameByID(int32(tag), msgDesc), u)

		case proto.WireFixed64:
			u, err = buffer.DecodeFixed64()
			if err != nil {
				fmt.Printf("t=%3d name=%s fix64 err %v\n", tag, getNameByID(int32(tag), msgDesc), err)
				break out
			}
			fmt.Fprintf(&output, "\"%s\": %d", getNameByID(int32(tag), msgDesc), u)
			//fmt.Printf("t=%3d name=%s fix64 %d\n", tag, getNameByID(int32(tag), msgDesc), u)

		case proto.WireVarint:
			u, err = buffer.DecodeVarint()
			if err != nil {
				fmt.Printf("t=%3d name=%s varint err %v\n", tag, getNameByID(int32(tag), msgDesc), err)
				break out
			}
			fmt.Fprintf(&output, "\"%s\": %d", getNameByID(int32(tag), msgDesc), u)
			//fmt.Printf("t=%3d name=%s varint %d\n", tag, getNameByID(int32(tag), msgDesc), u)
		}
	}

	output.WriteString("}")

	return output.Bytes(), nil
}

func Unmarshal(pb []byte, desc *descriptor.FileDescriptorProto, name string) ([]byte, error) {
	msgDesc := findMessageDescriptor(desc, name)
	if msgDesc == nil {
		return []byte{}, fmt.Errorf("can't find message '%s'", name)
	}

	return unmarshalMessage(proto.NewBuffer(pb), len(pb), 0, desc, msgDesc)
}
