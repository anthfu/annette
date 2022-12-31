(ns annette.server
  (:require [com.brunobonacci.mulog :as u])
  (:import (io.netty.bootstrap ServerBootstrap)
           (io.netty.buffer Unpooled)
           (io.netty.channel ChannelInitializer)
           (io.netty.channel ChannelFutureListener ChannelInboundHandlerAdapter ChannelOption)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.socket.nio NioServerSocketChannel)))

(defn echo-server-handler []
  (proxy [ChannelInboundHandlerAdapter] []
    (channelRead [ctx msg]
      (u/log ::channel-read)
      (.write ctx msg))
    (channelReadComplete [ctx]
      (u/log ::channel-read-complete)
      (-> ctx
          (.writeAndFlush Unpooled/EMPTY_BUFFER)
          (.addListener ChannelFutureListener/CLOSE)))
    (exceptionCaught [ctx e]
      (u/log ::channel-active :error (.getMessage e))
      (.close ctx))))

(defn start
  [port]
  (u/log ::server-init)
  (let [master-group (NioEventLoopGroup.)
        worker-group (NioEventLoopGroup.)]
    (try
      (let [b (ServerBootstrap.)]
        (-> b
            (.group master-group worker-group)
            (.channel NioServerSocketChannel)
            (.childHandler
              (proxy [ChannelInitializer] []
                (initChannel [ch] (-> ch
                                      (.pipeline)
                                      (.addLast (echo-server-handler))))))
            (.option ChannelOption/SO_BACKLOG (int 128))
            (.childOption ChannelOption/SO_KEEPALIVE true))
        (let [f (-> b
                    (.bind port)
                    (.sync))]
          (-> f
              (.channel)
              (.closeFuture)
              (.sync))))
      (finally
        (u/log ::server-shutdown)
        (.shutdownGracefully worker-group)
        (.shutdownGracefully master-group)))))

(defn -main [& _args]
  (u/start-publisher! {:type :console})
  (start 8080))
