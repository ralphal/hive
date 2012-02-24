/* Autogenerated by Thrift Compiler (0.8.0-dev)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package main

import (
        "flag"
        "fmt"
        "http"
        "net"
        "os"
        "strconv"
        "thrift"
        "thriftlib/fb303"
)

func Usage() {
  fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:\n")
  flag.PrintDefaults()
  fmt.Fprint(os.Stderr, "Functions:\n")
  fmt.Fprint(os.Stderr, "  getName() (retval138 string, err error)\n")
  fmt.Fprint(os.Stderr, "  getVersion() (retval139 string, err error)\n")
  fmt.Fprint(os.Stderr, "  getStatus() (retval140 FbStatus, err error)\n")
  fmt.Fprint(os.Stderr, "  getStatusDetails() (retval141 string, err error)\n")
  fmt.Fprint(os.Stderr, "  getCounters() (retval142 thrift.TMap, err error)\n")
  fmt.Fprint(os.Stderr, "  getCounter(key string) (retval143 int64, err error)\n")
  fmt.Fprint(os.Stderr, "  setOption(key string, value string) (err error)\n")
  fmt.Fprint(os.Stderr, "  getOption(key string) (retval145 string, err error)\n")
  fmt.Fprint(os.Stderr, "  getOptions() (retval146 thrift.TMap, err error)\n")
  fmt.Fprint(os.Stderr, "  getCpuProfile(profileDurationInSec int32) (retval147 string, err error)\n")
  fmt.Fprint(os.Stderr, "  aliveSince() (retval148 int64, err error)\n")
  fmt.Fprint(os.Stderr, "  reinitialize() (err error)\n")
  fmt.Fprint(os.Stderr, "  shutdown() (err error)\n")
  fmt.Fprint(os.Stderr, "\n")
  os.Exit(0)
}

func main() {
  flag.Usage = Usage
  var host string
  var port int
  var protocol string
  var urlString string
  var framed bool
  var useHttp bool
  var help bool
  var url http.URL
  var trans thrift.TTransport
  flag.Usage = Usage
  flag.StringVar(&host, "h", "localhost", "Specify host and port")
  flag.IntVar(&port, "p", 9090, "Specify port")
  flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
  flag.StringVar(&urlString, "u", "", "Specify the url")
  flag.BoolVar(&framed, "framed", false, "Use framed transport")
  flag.BoolVar(&useHttp, "http", false, "Use http")
  flag.BoolVar(&help, "help", false, "See usage string")
  flag.Parse()
  if help || flag.NArg() == 0 {
    flag.Usage()
  }
  
  if len(urlString) > 0 {
    url, err := http.ParseURL(urlString)
    if err != nil {
      fmt.Fprint(os.Stderr, "Error parsing URL: ", err.Error(), "\n")
      flag.Usage()
    }
    host = url.Host
    useHttp = len(url.Scheme) <= 0 || url.Scheme == "http"
  } else if useHttp {
    _, err := http.ParseURL(fmt.Sprint("http://", host, ":", port))
    if err != nil {
      fmt.Fprint(os.Stderr, "Error parsing URL: ", err.Error(), "\n")
      flag.Usage()
    }
  }
  
  cmd := flag.Arg(0)
  var err error
  if useHttp {
    trans, err = thrift.NewTHttpClient(url.Raw)
  } else {
    addr, err := net.ResolveTCPAddr("tcp", fmt.Sprint(host, ":", port))
    if err != nil {
      fmt.Fprint(os.Stderr, "Error resolving address", err.Error())
      os.Exit(1)
    }
    trans, err = thrift.NewTNonblockingSocketAddr(addr)
    if framed {
      trans = thrift.NewTFramedTransport(trans)
    }
  }
  if err != nil {
    fmt.Fprint(os.Stderr, "Error creating transport", err.Error())
    os.Exit(1)
  }
  defer trans.Close()
  var protocolFactory thrift.TProtocolFactory
  switch protocol {
  case "compact":
    protocolFactory = thrift.NewTCompactProtocolFactory()
    break
  case "simplejson":
    protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
    break
  case "json":
    protocolFactory = thrift.NewTJSONProtocolFactory()
    break
  case "binary", "":
    protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
    break
  default:
    fmt.Fprint(os.Stderr, "Invalid protocol specified: ", protocol, "\n")
    Usage()
    os.Exit(1)
  }
  client := fb303.NewFacebookServiceClientFactory(trans, protocolFactory)
  if err = trans.Open(); err != nil {
    fmt.Fprint(os.Stderr, "Error opening socket to ", host, ":", port, " ", err.Error())
    os.Exit(1)
  }
  
  switch cmd {
  case "getName":
    if flag.NArg() - 1 != 0 {
      fmt.Fprint(os.Stderr, "GetName requires 0 args\n")
      flag.Usage()
    }
    fmt.Print(client.GetName())
    fmt.Print("\n")
    break
  case "getVersion":
    if flag.NArg() - 1 != 0 {
      fmt.Fprint(os.Stderr, "GetVersion requires 0 args\n")
      flag.Usage()
    }
    fmt.Print(client.GetVersion())
    fmt.Print("\n")
    break
  case "getStatus":
    if flag.NArg() - 1 != 0 {
      fmt.Fprint(os.Stderr, "GetStatus requires 0 args\n")
      flag.Usage()
    }
    fmt.Print(client.GetStatus())
    fmt.Print("\n")
    break
  case "getStatusDetails":
    if flag.NArg() - 1 != 0 {
      fmt.Fprint(os.Stderr, "GetStatusDetails requires 0 args\n")
      flag.Usage()
    }
    fmt.Print(client.GetStatusDetails())
    fmt.Print("\n")
    break
  case "getCounters":
    if flag.NArg() - 1 != 0 {
      fmt.Fprint(os.Stderr, "GetCounters requires 0 args\n")
      flag.Usage()
    }
    fmt.Print(client.GetCounters())
    fmt.Print("\n")
    break
  case "getCounter":
    if flag.NArg() - 1 != 1 {
      fmt.Fprint(os.Stderr, "GetCounter requires 1 args\n")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.GetCounter(value0))
    fmt.Print("\n")
    break
  case "setOption":
    if flag.NArg() - 1 != 2 {
      fmt.Fprint(os.Stderr, "SetOption requires 2 args\n")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    fmt.Print(client.SetOption(value0, value1))
    fmt.Print("\n")
    break
  case "getOption":
    if flag.NArg() - 1 != 1 {
      fmt.Fprint(os.Stderr, "GetOption requires 1 args\n")
      flag.Usage()
    }
    argvalue0 := flag.Arg(1)
    value0 := argvalue0
    fmt.Print(client.GetOption(value0))
    fmt.Print("\n")
    break
  case "getOptions":
    if flag.NArg() - 1 != 0 {
      fmt.Fprint(os.Stderr, "GetOptions requires 0 args\n")
      flag.Usage()
    }
    fmt.Print(client.GetOptions())
    fmt.Print("\n")
    break
  case "getCpuProfile":
    if flag.NArg() - 1 != 1 {
      fmt.Fprint(os.Stderr, "GetCpuProfile requires 1 args\n")
      flag.Usage()
    }
    tmp0, err155 := (strconv.Atoi(flag.Arg(1)))
    if err155 != nil {
      Usage()
      return
    }
    argvalue0 := int32(tmp0)
    value0 := argvalue0
    fmt.Print(client.GetCpuProfile(value0))
    fmt.Print("\n")
    break
  case "aliveSince":
    if flag.NArg() - 1 != 0 {
      fmt.Fprint(os.Stderr, "AliveSince requires 0 args\n")
      flag.Usage()
    }
    fmt.Print(client.AliveSince())
    fmt.Print("\n")
    break
  case "reinitialize":
    if flag.NArg() - 1 != 0 {
      fmt.Fprint(os.Stderr, "Reinitialize requires 0 args\n")
      flag.Usage()
    }
    fmt.Print(client.Reinitialize())
    fmt.Print("\n")
    break
  case "shutdown":
    if flag.NArg() - 1 != 0 {
      fmt.Fprint(os.Stderr, "Shutdown requires 0 args\n")
      flag.Usage()
    }
    fmt.Print(client.Shutdown())
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprint(os.Stderr, "Invalid function ", cmd, "\n")
  }
}
