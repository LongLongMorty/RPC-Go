package main

import (
	"flag"
	"kamaRPC/internal/codec"
	"kamaRPC/internal/config"
	"kamaRPC/internal/registry"
	"kamaRPC/internal/server"
	"kamaRPC/pkg/api"
	"log"
	"os"
	"os/signal"
)

func main() {
	etcdFlag := flag.String("etcd", "", "etcd 地址，多个用逗号分隔（环境变量 KAMARPC_ETCD）")
	listenFlag := flag.String("listen", "", "监听地址（环境变量 KAMARPC_LISTEN）")
	advertiseFlag := flag.String("advertise", "", "注册到 etcd 的地址（环境变量 KAMARPC_ADVERTISE）")
	flag.Parse()

	etcdAddr := config.Get(*etcdFlag, "KAMARPC_ETCD", "localhost:2379")
	listenAddr := config.Get(*listenFlag, "KAMARPC_LISTEN", ":9091")
	advertiseAddr := config.Get(*advertiseFlag, "KAMARPC_ADVERTISE", "localhost:9091")

	reg, err := registry.NewRegistry(config.Endpoints(etcdAddr))
	if err != nil {
		log.Fatal(err)
	}

	srv, err := server.NewServer(listenAddr, server.WithServerCodec(codec.JSON))
	if err != nil {
		log.Println("server.NewServer error ", err.Error())
		return
	}
	// 注册 Arith 服务
	srv.Register("Arith", &api.Arith{})

	// 注册服务到 etcd
	err = reg.Register("Arith", registry.Instance{
		Addr: advertiseAddr,
	}, 10)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("server started at", listenAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	go func() {
		if err := srv.Start(); err != nil {
			log.Println("server start error:", err)
		}
	}()

	<-sigCh
	log.Println("graceful shutdown...")
	srv.Shutdown()
	reg.Close()
}
