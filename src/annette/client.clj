(ns annette.client
  (:require [com.brunobonacci.mulog :as u])
  (:import (io.netty.bootstrap Bootstrap)
           (io.netty.buffer Unpooled)
           (io.netty.channel ChannelInitializer ChannelOption SimpleChannelInboundHandler)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.socket.nio NioSocketChannel)
           (io.netty.util CharsetUtil)))

(defn echo-client-handler
  []
  (proxy [SimpleChannelInboundHandler] []
    (channelActive [ctx]
      (let [msg (Unpooled/copiedBuffer "Hello world!" CharsetUtil/UTF_8)]
        (u/log ::channel-active)
        (.writeAndFlush ctx msg)))
    (channelRead0 [_ctx in]
      (u/log ::channel-read0 :message (.toString in CharsetUtil/UTF_8)))
    (exceptionCaught [ctx e]
      (u/log ::channel-active :error (.getMessage e))
      (.close ctx))))

(defn run
  [host port]
  (u/log ::client-init)
  (let [worker-group (NioEventLoopGroup.)]
    (try
      (let [b (Bootstrap.)]
        (-> b
            (.group worker-group)
            (.channel NioSocketChannel)
            (.option ChannelOption/SO_KEEPALIVE true)
            (.handler
              (proxy [ChannelInitializer] []
                (initChannel [ch] (-> ch
                                      (.pipeline)
                                      (.addLast (echo-client-handler)))))))
        (let [f (-> b
                    (.connect host port)
                    (.sync))]
          (-> f
              (.channel)
              (.closeFuture)
              (.sync))))
      (finally
        (.shutdownGracefully worker-group)))))

(defn -main [& _args]
  (u/start-publisher! {:type :console})
  (run "localhost" 8080))
