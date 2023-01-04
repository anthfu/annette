(ns annette.client
  (:require [com.brunobonacci.mulog :as u])
  (:import (io.netty.bootstrap Bootstrap)
           (io.netty.buffer Unpooled)
           (io.netty.channel ChannelInitializer ChannelOption SimpleChannelInboundHandler)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.socket.nio NioSocketChannel)
           (io.netty.util CharsetUtil)))

(defn echo-client-handler
  [text]
  (proxy [SimpleChannelInboundHandler] []
    (channelActive [ctx]
      (let [msg (Unpooled/copiedBuffer text CharsetUtil/UTF_8)]
        (u/log ::channel-active)
        (.writeAndFlush ctx msg)))
    (channelRead0 [_ctx in]
      (let [msg (.toString in CharsetUtil/UTF_8)]
        (u/log ::channel-read0 :message msg)
        (println msg)))
    (exceptionCaught [ctx e]
      (u/log ::channel-active :error (.getMessage e))
      (.close ctx))))

(defn echo
  [host port text]
  (u/start-publisher! {:type :console})
  (u/log ::client-init)
  (let [worker-group (NioEventLoopGroup.)]
    (try
      (let [bootstrap (-> (Bootstrap.)
                          (.group worker-group)
                          (.channel NioSocketChannel)
                          (.option ChannelOption/SO_KEEPALIVE true)
                          (.handler
                            (proxy [ChannelInitializer] []
                              (initChannel [ch]
                                (-> ch
                                    (.pipeline)
                                    (.addLast (echo-client-handler text)))))))
            fut (-> bootstrap
                    (.connect host port)
                    (.sync))]
        (-> fut
            (.channel)
            (.closeFuture)
            (.sync)))
      (finally
        (u/log ::client-shutdown)
        (.shutdownGracefully worker-group)))))
