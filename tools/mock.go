package tools

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strings"

	"golang.org/x/tools/imports"
)



func MockInterfaces(pkgOut, outDir string, repos map[string]interface{}) {
	os.MkdirAll(outDir, os.ModePerm)

	for name, repo := range repos {
		builder := mockGen(repo, pkgOut)

		res, err := imports.Process("", []byte(builder.String()), &imports.Options{Comments: true})
		if err != nil {
			panic(err)
		}

		err = ioutil.WriteFile(outDir+"/"+name+".go", res, 0644)
		if err != nil {
			log.Println(err)
			panic(err)
		}
		log.Print("Mock gen " + outDir + "/" + name + ".go")
	}
}

func mockGen(in interface{}, pkgName string) strings.Builder {
	typ := reflect.TypeOf(in)
	name := "Mock" + typ.Elem().Name()

	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf(`// Code generated by mockgen. DO NOT EDIT.
package %s
import (
	"github.com/stretchr/testify/mock"
)
type %s struct {
	mock.Mock
}
`, pkgName, name))

	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		methodType := method.Type.String()[4:len(method.Type.String())]

		count := 0

		var (
			startAt, endAt        int
			inputArgs, returnArgs string
		)
		for i, char := range methodType {
			if char == '(' {
				count++
				if count == 1 {
					startAt = i + 1
				}
			}

			if char == ')' {
				count--
			}

			if count == 0 {
				endAt = i

				returnArgs = strings.Trim(methodType[endAt+1:], " ")
				inputArgs = methodType[startAt:endAt]
				break
			}
		}

		args := strings.Split(inputArgs, ",")

		inputArgs = ""

		argsName := []string{}
		argsNameAndType := []string{}
		//skip arg1 because this is receiver
		for i, argType := range args[1:] {
			argName := fmt.Sprintf("arg%d", i+1)
			argsNameAndType = append(argsNameAndType, argName+" "+argType)
			argsName = append(argsName, argName)
		}

		preReturnGroup := ""
		returnGroup := []string{}
		parts := strings.Split(returnArgs, ",")
		for i, arg := range parts {
			arg = strings.Trim(arg, "( )")

			if arg == "" {
				continue
			} else if arg == "error" {
				returnGroup = append(returnGroup, fmt.Sprintf("args.Error(%d)", i))
			} else {
				returnGroup = append(returnGroup, fmt.Sprintf("args.Get(%d).(%s)", i, arg))
			}
		}

		returnArgsString := "return " + strings.Join(returnGroup, ", ")

		if len(returnGroup) > 0 {
			preReturnGroup = fmt.Sprintf("args := r.Called(%s)", strings.Join(argsName, ", "))
		} else {
			preReturnGroup = fmt.Sprintf("_ = r.Called(%s)", strings.Join(argsName, ", "))
		}

		for i, arg := range returnGroup {
			if strings.Contains(arg, "[]") || strings.Contains(arg, "*") {
				returnGroup[i] = "nil"
				preReturnGroup += fmt.Sprintf(`
	if args.Get(%d) == nil {
		return %s
	}`, i, strings.Join(returnGroup, ", "))

			}
		}

		builder.WriteString(fmt.Sprintf(`
func (r *%s) %s (%s) %s {
	%s
	%s
}
`, name, typ.Method(i).Name, strings.Join(argsNameAndType, ", "), returnArgs, preReturnGroup, returnArgsString))
	}
	return builder
}
