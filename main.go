package main

import (
    "context"
    "fmt"
    "log"
    "net"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/spf13/pflag"
)

var opts struct {
    Host   string
    Port   uint16
    File   string
    Buffer int
}

func init() {
    pflag.StringVar(&opts.Host, "host", "0.0.0.0", "IP to bind to")
    pflag.Uint16Var(&opts.Port, "port", 2202, "UDP port to bind to")
    pflag.StringVar(&opts.File, "file", "", "dump received data to a dump file")
    pflag.IntVar(&opts.Buffer, "buffer", 10240, "max buffer size for the socket io")
}

func newUDPListener(host string, port uint16) (*net.UDPConn, error) {
    addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", host, port))
    if err != nil {
        return nil, err
    }
    conn, err := net.ListenUDP("udp", addr)
    if err != nil {
        return nil, err
    }
    return conn, nil
}

func handleClient(ctx context.Context, conn *net.UDPConn) {
    buffer := make([]byte, opts.Buffer)
    for {
        select {
        case <-ctx.Done():
            return
        default:
            conn.SetReadDeadline(time.Now().Add(5 * time.Second))
            n, addr, err := conn.ReadFromUDP(buffer)
            if err != nil && !strings.Contains(err.Error(), "i/o timeout") {
                log.Printf("Read from UDP failed, err: %v", err)
                return
            }
            if err == nil {
                log.Printf("Read from client(%v:%v), len: %v, [%v]", addr.IP, addr.Port, n, string(buffer[:n]))

                if len(opts.File) != 0 {
                    f, err := os.OpenFile(opts.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
                    if err != nil {
                        log.Printf("Open file failed, err: %v", err)
                        return
                    }
                    defer f.Close()
                    if _, err = f.Write(buffer[:n]); err != nil {
                        log.Printf("Write file failed, err: %v", err)
                        return
                    }
                }

                conn.WriteToUDP(buffer[:n], addr)
            }
        }
    }
}

func main() {
    pflag.Parse()

    sigChannel := make(chan os.Signal, 1)
    signal.Notify(sigChannel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    conn, err := newUDPListener(opts.Host, opts.Port)
    if err != nil {
        log.Fatalf("Failed to create UDP listener: %v", err)
    }
    defer conn.Close()

    log.Printf(">> Starting udpdump, listening at %s:%d...", opts.Host, opts.Port)

    go handleClient(ctx, conn)

    sig := <-sigChannel
    log.Printf("Received signal [%v], shutting down...", sig)
}

