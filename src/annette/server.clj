(ns annette.server
  (:require [com.brunobonacci.mulog :as u])
  (:import (io.netty.bootstrap ServerBootstrap)
           (io.netty.buffer Unpooled)
           (io.netty.channel ChannelInitializer)
           (io.netty.channel ChannelFutureListener ChannelInboundHandlerAdapter ChannelOption)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.socket.nio NioServerSocketChannel)
           (io.netty.util CharsetUtil)))

(defn echo-server-handler []
  (proxy [ChannelInboundHandlerAdapter] []
    (channelRead [ctx in]
      (u/log ::channel-read :message (.toString in CharsetUtil/UTF_8))
      (.write ctx in))
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
  (u/start-publisher! {:type :console})
  (u/log ::server-init)
  (let [master-group (NioEventLoopGroup.)
        worker-group (NioEventLoopGroup.)]
    (try
      (let [bootstrap (-> (ServerBootstrap.)
                          (.group master-group worker-group)
                          (.channel NioServerSocketChannel)
                          (.childHandler
                            (proxy [ChannelInitializer] []
                              (initChannel [ch]
                                (-> ch
                                    (.pipeline)
                                    (.addLast (echo-server-handler))))))
                          (.option ChannelOption/SO_BACKLOG (int 128))
                          (.childOption ChannelOption/SO_KEEPALIVE true))
            fut (-> bootstrap
                    (.bind port)
                    (.sync))]
        (-> fut
            (.channel)
            (.closeFuture)
            (.sync)))
      (finally
        (u/log ::server-shutdown)
        (.shutdownGracefully worker-group)
        (.shutdownGracefully master-group)))))

(defn -main [& _args]
  (start 8080))
